
/*
 * s3backer - FUSE-based single file backing store via Amazon S3
 * 
 * Copyright 2008-2011 Archie L. Cobbs <archie@dellroad.org>
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 *
 * In addition, as a special exception, the copyright holders give
 * permission to link the code of portions of this program with the
 * OpenSSL library under certain conditions as described in each
 * individual source file, and distribute linked combinations including
 * the two.
 *
 * You must obey the GNU General Public License in all respects for all
 * of the code used other than OpenSSL. If you modify file(s) with this
 * exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do
 * so, delete this exception statement from your version. If you delete
 * this exception statement from all source files in the program, then
 * also delete it here.
 */

#include "s3backer.h"
#include "mem_cache.h"
#include "hash.h"
#include "math.h"

/*
 * 通过合并文件系统小块（4k）的请求为大块请求，提高读写性能
 * mem_entry state:
 *
 *  CLEAN       Data is consistent with underlying block_chche
 *  DIRTY       Data is inconsistent with underlying block_chche
 *  READING     Data is being read from block_chche
 *  WRITING     Data is being written to block_chche
 */
#define CLEAN           0
#define DIRTY           1
#define READING         2
#define WRITING         3

/* Configuration info structure for min_block */
struct min_block {
	off_t                           offset;         // offset
	size_t                          size;           // size of min_block entry
	u_int                           state;        // state, 0:clean 1:dirty
    pthread_mutex_t                 min_mutex;      // my mutex
};

/* Configuration info structure for mem_entry */
struct mem_entry {
	s3b_block_t            block_num;      // block number
	u_int                  state;          // 0:CLEAN 1:DIRTY 2:READING 3:WRITING
	struct min_block       (*array_min)[0];  // array of min_block
    TAILQ_ENTRY(mem_entry) link;           // next in list (cleans or dirties)
    u_int                  num_dirties;    // min_block's number that are dirty
    uint64_t               update_time;    // last update time
    u_char                 *buf;           // buffer in memroy
    pthread_mutex_t        mem_mutex;      // my mutex
};

/*针对mem_entry的链表宏定义*/
#define ENTRY_IN_LIST(entry)                ((entry)->link.tqe_prev != NULL)
#define ENTRY_RESET_LINK(entry)             do { (entry)->link.tqe_prev = NULL; } while (0)
#define ENTRY_GET_STATE(entry)              ((entry)->state == 0 ?                       \
		                                         CLEAN : ((entry)->state == 1 ?          \
		                                         DIRTY : ((entry)->state == 2 ? READING : WRITING)))

/* One time unit in milliseconds */
#define TIME_UNIT_MILLIS            64

/* The queue ratio at which we want to be writing out blocks or free memory immediately */
#define DIRTY_RATIO_WRITE_ASAP      0.90            // 90%

/* Private data */
struct mem_cache_private {
    struct mem_cache_conf           *config;        // configuration
    struct s3backer_store           *inner;         // underlying s3backer store
    double                          max_dirty_ratio;// dirty ratio at which we write immediately
    u_int                           num_threads;    // number of alive worker threads
    TAILQ_HEAD(, mem_entry)         queue;          // list of clean/dirty blocks (write order)
    struct s3b_hash                 *hashtable;     // hashtable of all cached blocks
    int                             stopping;       // signals worker threads to exit
    pthread_mutex_t                 pri_mutex;      // my mutex
    pthread_cond_t                  pri_worker_work;    // there is new work for worker thread(s)
    pthread_cond_t                  pri_worker_exit;    // a worker thread has exited
    pthread_cond_t                  pri_end_reading;    // some entry in state READING[2] changed state
    pthread_cond_t                  pri_write_complete; // a write has completed
    pthread_cond_t                  pri_space_avail;    // there is new space available in memory
};

/* Callback info */
struct cbinfo {
    block_list_func_t           *callback;
    void                        *arg;
};

/* s3backer_store functions */
static int mem_cache_meta_data(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep);
static int mem_cache_set_mounted(struct s3backer_store *s3b, int *old_valuep, int new_value);
static int mem_cache_read_block(struct s3backer_store *s3b, s3b_block_t block_num, void *dest,
  u_char *actual_md5, const u_char *expect_md5, int strict);
static int mem_cache_write_block(struct s3backer_store *s3b, s3b_block_t block_num, const void *src, u_char *md5,
  check_cancel_t *check_cancel, void *check_cancel_arg);
static int mem_cache_read_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, void *dest);
static int mem_cache_write_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, const void *src);
static int mem_cache_list_blocks(struct s3backer_store *s3b, block_list_func_t *callback, void *arg);
static int mem_cache_flush(struct s3backer_store *s3b);
static void mem_cache_destroy(struct s3backer_store *s3b);

/* Other functions */
//static s3b_dcache_visit_t mem_cache_dcache_load;
static int mem_cache_read(struct mem_cache_private *priv, s3b_block_t block_num, u_int off, u_int len, void *dest);
static int mem_cache_write(struct mem_cache_private *priv, s3b_block_t block_num, u_int off, u_int len, const void *src);
static void *mem_cache_worker_main(void *arg);
static void mem_cache_free_one(void *arg, void *value);
static int get_mem_cache_data(struct mem_cache_private *priv, struct mem_entry *entry, const void *src, u_int off, u_int len);
static uint64_t mem_cache_get_time_millis(void);
static uint32_t mem_cache_get_time(struct mem_cache_private *priv);
static void mem_cache_worker_wait(struct mem_cache_private *priv, struct mem_entry *entry);
static int mem_cache_get_entry(struct mem_cache_private *priv, struct mem_entry **entryp);
static int mem_cache_read_inner(struct mem_cache_private *priv, s3b_block_t block_num,struct mem_entry *entry);
static double mem_cache_dirty_ratio(struct mem_cache_private *priv, struct mem_entry *entry);

/* Invariants checking */
#ifndef NDEBUG
static void mem_cache_check_invariants(struct mem_cache_private *priv);
static void mem_cache_check_one(void *arg, void *value);
#define S3BCACHE_CHECK_INVARIANTS(priv)     mem_cache_check_invariants(priv)
#else
#define S3BCACHE_CHECK_INVARIANTS(priv)     do { } while (0)
#endif

/*
 * Wrap an underlying s3backer store with a block cache. Invoking the
 * destroy method will destroy both this and the inner s3backer store.
 *
 * Returns NULL and sets errno on failure.
 */
struct s3backer_store *
mem_cache_create(struct mem_cache_conf *config, struct s3backer_store *inner)
{
    struct s3backer_store *s3b;
    struct mem_cache_private *priv;
    struct mem_entry *entry;
    pthread_t thread;
    int r;
    u_int hash_length;

    /* Initialize s3backer_store structure */
    if ((s3b = calloc(1, sizeof(*s3b))) == NULL) {
        r = errno;
        (*config->log)(LOG_ERR, "calloc(): %s", strerror(r));
        goto fail0;
    }
    s3b->meta_data = mem_cache_meta_data;
    s3b->set_mounted = mem_cache_set_mounted;
    s3b->read_block = mem_cache_read_block;
    s3b->write_block = mem_cache_write_block;
    s3b->read_block_part = mem_cache_read_block_part;
    s3b->write_block_part = mem_cache_write_block_part;
    s3b->list_blocks = mem_cache_list_blocks;
    s3b->flush = mem_cache_flush;
    s3b->destroy = mem_cache_destroy;

    /* Initialize block_cache_private structure */
    if ((priv = calloc(1, sizeof(*priv))) == NULL) {
        r = errno;
        (*config->log)(LOG_ERR, "calloc(): %s", strerror(r));
        goto fail1;
    }

    priv->config = config;
    priv->inner = inner;

    /* Compute dirty ratio at which we will be writing immediately */
	priv->max_dirty_ratio = (double)config->min_max_dirty / (double)config->min_num_blocks;
	if (priv->max_dirty_ratio > DIRTY_RATIO_WRITE_ASAP)
		priv->max_dirty_ratio = DIRTY_RATIO_WRITE_ASAP;

    if ((r = pthread_mutex_init(&priv->pri_mutex, NULL)) != 0)
        goto fail2;
	if ((r = pthread_cond_init(&priv->pri_worker_work, NULL)) != 0)
		goto fail3;
	if ((r = pthread_cond_init(&priv->pri_worker_exit, NULL)) != 0)
		goto fail4;
    if ((r = pthread_cond_init(&priv->pri_end_reading, NULL)) != 0)
        goto fail5;
    if ((r = pthread_cond_init(&priv->pri_write_complete, NULL)) != 0)
        goto fail6;
	if ((r = pthread_cond_init(&priv->pri_space_avail, NULL)) != 0)
		goto fail7;
    TAILQ_INIT(&priv->queue);

    /* Create hash table */
    u_int multiple = config->mem_block_size / config->block_size;
	hash_length = ceil((double)config->cache_size / multiple);
    if ((r = s3b_hash_create(&priv->hashtable, hash_length)) != 0)
        goto fail8;
    s3b->data = priv;

    /* Create threads */
    pthread_mutex_lock(&priv->pri_mutex);
    S3BCACHE_CHECK_INVARIANTS(priv);
    for (priv->num_threads = 0; priv->num_threads < config->mem_max_threads; priv->num_threads++) {
        if ((r = pthread_create(&thread, NULL, mem_cache_worker_main, priv)) != 0)
            goto fail9;
    }
    pthread_mutex_unlock(&priv->pri_mutex);
    return s3b;

fail9:
    priv->stopping = 1;
    while (priv->num_threads > 0) {
        pthread_cond_broadcast(&priv->pri_worker_work);
        pthread_cond_wait(&priv->pri_worker_exit, &priv->pri_mutex);
    }
	while ((entry = TAILQ_FIRST(&priv->queue)) != NULL) {
		TAILQ_REMOVE(&priv->queue, entry, link);
		free(entry);
	}
    s3b_hash_destroy(priv->hashtable);
fail8:
	pthread_cond_destroy(&priv->pri_space_avail);
fail7:
	pthread_cond_destroy(&priv->pri_write_complete);
fail6:
	pthread_cond_destroy(&priv->pri_end_reading);
fail5:
    pthread_cond_destroy(&priv->pri_worker_exit);
fail4:
    pthread_cond_destroy(&priv->pri_worker_work);
fail3:
    pthread_mutex_destroy(&priv->pri_mutex);
fail2:
    free(priv);
fail1:
    free(s3b);
fail0:
    (*config->log)(LOG_ERR, "mem_cache creation failed: %s", strerror(r));
    errno = r;
    return NULL;
}

/*
 * Worker thread main entry point.
 */
static void *
mem_cache_worker_main(void *arg)
{
	struct mem_cache_private *const priv = arg;
	struct mem_cache_conf *const config = priv->config;
	struct mem_entry *entry;
    u_int multiple = config->mem_block_size / config->block_size;
    u_int cache_block_num;
    uint64_t time_slot;
    void *buf;
    int r;
    int i;
    int flag = 0;

    /* Grab lock */
    pthread_mutex_lock(&priv->pri_mutex);

    if ((buf = malloc(config->block_size)) == NULL) {
		(*config->log)(LOG_ERR, "Can't allocate buffer, exiting: %s", strerror(errno));
		goto done;
	}
    while (1) {
//    	(*config->log)(LOG_DEBUG, "..................................");
        if ((entry = TAILQ_FIRST(&priv->queue)) != NULL) {
        	time_slot = mem_cache_get_time_millis() - entry->update_time;
        	if (ENTRY_GET_STATE(entry) == READING) {
        		TAILQ_REMOVE(&priv->queue, entry, link);
        		TAILQ_INSERT_TAIL(&priv->queue, entry, link);
        		continue;
			}
//			(*config->log)(LOG_DEBUG, "time = %u   timeout = %u",time_slot/TIME_UNIT_MILLIS,config->mem_timeout);
			if (mem_cache_dirty_ratio(priv, entry) > priv->max_dirty_ratio || time_slot/TIME_UNIT_MILLIS >= config->mem_timeout || priv->stopping) {
//				(*config->log)(LOG_DEBUG, ">>>>>>>>>>%u",entry->block_num);
				if (ENTRY_GET_STATE(entry) == CLEAN) {
					TAILQ_REMOVE(&priv->queue, entry, link);
					goto free;
				}
				if (ENTRY_GET_STATE(entry) == DIRTY) {
					TAILQ_REMOVE(&priv->queue, entry, link);
					entry->state = WRITING;
					entry->update_time = 0;
					assert(ENTRY_GET_STATE(entry) == WRITING);

					cache_block_num = entry->block_num * multiple;
					for (i = 0; i < multiple; i++) {
						memcpy((char *)buf, (char *)entry->buf + i * config->block_size, config->block_size);

						pthread_mutex_unlock(&priv->pri_mutex);
						r = (*priv->inner->write_block)(priv->inner, cache_block_num, buf, NULL, NULL, NULL);

						pthread_mutex_lock(&priv->pri_mutex);
						if (r != 0)
							flag = 1;
						cache_block_num++;
					}

					assert(ENTRY_GET_STATE(entry) == WRITING);
					if (flag) {    // fail
						entry->state = DIRTY;
						TAILQ_INSERT_HEAD(&priv->queue, entry, link);
						continue;
					}
					goto free;
				}
free:
				s3b_hash_remove(priv->hashtable, entry->block_num);
				pthread_cond_signal(&priv->pri_space_avail);
				pthread_cond_broadcast(&priv->pri_write_complete);
				free(entry->buf);
				free(entry->array_min);
				free(entry);
				continue;
			}
		}
        /* 退出循环 */
		if (priv->stopping != 0)
			break;
		/* 线程休眠，等待唤醒 */
    	mem_cache_worker_wait(priv, entry);
    }
    /* 减少当前存活线程数目 */
    priv->num_threads--;
    (*config->log)(LOG_DEBUG, ".......alive_thread : %u",priv->num_threads);
    pthread_cond_signal(&priv->pri_worker_exit);

done:
    pthread_mutex_unlock(&priv->pri_mutex);
    free(buf);
    return NULL;
}

static int
mem_cache_write_block(struct s3backer_store *const s3b, s3b_block_t block_num, const void *src, u_char *md5,
  check_cancel_t *check_cancel, void *check_cancel_arg)
{
    struct mem_cache_private *const priv = s3b->data;
    struct mem_cache_conf *const config = priv->config;
    s3b_block_t mem_block_num;
    size_t fragoff;
    u_int multiple;
    u_int cache_bSize = config->block_size;
    int r;

    pthread_mutex_lock(&priv->pri_mutex);

    assert(md5 == NULL);
    multiple = config->mem_block_size / cache_bSize;
	mem_block_num = block_num / multiple;
	fragoff = (block_num % multiple) * (size_t)cache_bSize;
	(*config->log)(LOG_DEBUG, "mem_cache_write_block>>>>>>>> mem_block_num=%u fragoff=0x%jx",
			mem_block_num,(uintmax_t)fragoff);

	//写入长度为block_size数据到大块
	r = mem_cache_write(priv, mem_block_num, fragoff, cache_bSize, src);
	pthread_mutex_unlock(&priv->pri_mutex);
	return r;
//    return (*priv->inner->write_block)(priv->inner,block_num,src,md5,check_cancel,check_cancel_arg);
}

static int
mem_cache_write_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, const void *src)
{
    struct mem_cache_private *const priv = s3b->data;
    struct mem_cache_conf *const config = priv->config;
    s3b_block_t mem_block_num;
    size_t fragoff;
    u_int multiple;
    u_int cache_bSize = config->block_size;
    int r;
//    (*config->log)(LOG_DEBUG, "mem_write1>>>>>>>> block_num=%u off=0x%jx len=0x%jx",block_num,(uintmax_t)off, (uintmax_t)len);
    pthread_mutex_lock(&priv->pri_mutex);

    multiple = config->mem_block_size / cache_bSize;
    mem_block_num = block_num / multiple;
    fragoff = (size_t)off + (block_num % multiple) * (size_t)cache_bSize;
	(*config->log)(LOG_DEBUG, "mem_cache_write_block_part >>>>>>>>>>> mem_block_num=%u fragoff=0x%jx len=0x%jx",
			mem_block_num,(uintmax_t)fragoff, (uintmax_t)len);

	//写入数据到大块
	r = mem_cache_write(priv, mem_block_num, fragoff, len, src);
	pthread_mutex_unlock(&priv->pri_mutex);
	return r;
//    return (*priv->inner->write_block_part)(priv->inner,block_num,off,len,src);
}

/*
 * Write a memory block
 */
static int
mem_cache_write(struct mem_cache_private *const priv, s3b_block_t block_num, u_int off, u_int len, const void *src)
{
    struct mem_cache_conf *const config = priv->config;
    struct mem_entry *entry;
    struct min_block *min_entry;
    int flag=0;
    int r;
    int i;
    u_int min_entry_num;
    u_int min_bNum_begin;
    u_int min_bNum_end;
    min_entry_num = ceil((double)config->mem_block_size / config->min_block_size);
    min_bNum_begin = floor((double)off / config->min_block_size);
    min_bNum_end = ceil((double)(off + len) / config->min_block_size);

    /* Sanity check */
    assert(off <= config->mem_block_size);
    assert(len <= config->mem_block_size);
    assert(off + len <= config->mem_block_size);
again:
    /* Find mem_entry */
    if ((entry = s3b_hash_get(priv->hashtable, block_num)) != NULL) {
        assert(entry->block_num == block_num);
		switch (ENTRY_GET_STATE(entry)) {
		case READING:               /* wait for entry to leave READING */
			pthread_cond_wait(&priv->pri_end_reading, &priv->pri_mutex);
			goto again;
		case WRITING:               /* wait for entry to leave WRITING */
			pthread_cond_wait(&priv->pri_write_complete, &priv->pri_mutex);
			goto again;
		case CLEAN:
			/* If there are too many dirty blocks, we have to wait */
			if (entry->num_dirties >= config->min_max_dirty) {
				pthread_cond_signal(&priv->pri_worker_work);
				pthread_cond_wait(&priv->pri_write_complete, &priv->pri_mutex);
				goto again;
			}
			flag = 1;
			// FALLTHROUGH
		case DIRTY:
			for (i = 0; i < (min_bNum_end - min_bNum_begin); i++) {
				min_entry = entry->array_min[min_bNum_begin + i];
//				pthread_mutex_lock(&min_entry->min_mutex);
			}
			if ((r = get_mem_cache_data(priv, entry, src, off, len)) != 0) {
				(*config->log)(LOG_ERR, "error writing mem_cache block! %s", strerror(r));
				return r;
			}
		    if (entry == NULL) {
		        pthread_cond_wait(&priv->pri_space_avail, &priv->pri_mutex);
		        goto again;
		    }
//			for (i = 0; i < (min_bNum_end - min_bNum_begin); i++) {
//				pthread_mutex_unlock(&min_entry->min_mutex);
//			}

//			pthread_mutex_lock(&entry->mem_mutex);
			entry->update_time = mem_cache_get_time_millis();
			if (flag) {    //clean
				for (i = 0; i < (min_bNum_end - min_bNum_begin); i++) {
					min_entry = entry->array_min[min_bNum_begin + i];
					min_entry->state = DIRTY;
				}
				entry->state = DIRTY;
				entry->num_dirties += min_bNum_end - min_bNum_begin;
			} else {       //dirty
				if (ENTRY_GET_STATE(min_entry) != DIRTY) {
					for (i = 0; i < (min_bNum_end - min_bNum_begin); i++) {
						min_entry = entry->array_min[min_bNum_begin + i];
						min_entry->state = DIRTY;
					}
					entry->num_dirties += min_bNum_end - min_bNum_begin;
				}
			}
//			pthread_mutex_unlock(&entry->mem_mutex);
			break;
		default:
			assert(0);
			break;
		}
		goto done;
    }
	/* 新建mem_entry及数据buffer */
	if ((r = mem_cache_get_entry(priv, &entry)) != 0) {
		pthread_cond_wait(&priv->pri_space_avail, &priv->pri_mutex);
		goto again;
	}
    //从磁盘缓存读取数据到mem_entry
    if ((r = mem_cache_read_inner(priv,block_num,entry)) != 0)
    	(*config->log)(LOG_ERR, "error read mem_entry from cache_entry! %s", strerror(r));
    goto again;
done:
	pthread_cond_signal(&priv->pri_worker_work);
    return 0;
}

static int
mem_cache_read_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, void *dest)
{
    struct mem_cache_private *const priv = s3b->data;
    struct mem_cache_conf *const config = priv->config;
    s3b_block_t mem_block_num;
    size_t fragoff;
    u_int multiple;
    int r;
//	(*config->log)(LOG_DEBUG, "mem_read000>>>>>>>> block_num=%u fragoff=0x%jx fraglen=0x%jx", block_num,(uintmax_t)off, (uintmax_t)len);

    /* Grab lock */
	pthread_mutex_lock(&priv->pri_mutex);

    multiple = config->mem_block_size / config->block_size;
    mem_block_num = block_num / multiple;
    fragoff = (size_t)off + (block_num % multiple) * (size_t)config->block_size;
	(*config->log)(LOG_DEBUG, "mem_cache_read_block_part >>>>>>>>>>> mem_block_num=%u fragoff=0x%jx len=0x%jx",
			mem_block_num,(uintmax_t)fragoff, (uintmax_t)len);
	//从大块读取小块到dest，注：读取长度小于磁盘缓存块大小
	r = mem_cache_read(priv, mem_block_num, fragoff, len, dest);

	/* Release lock */
	pthread_mutex_unlock(&priv->pri_mutex);
	return r;
//    return (*priv->inner->read_block_part)(priv->inner,block_num,off,len,dest);
}

static int
mem_cache_read_block(struct s3backer_store *const s3b, s3b_block_t block_num, void *dest,
  u_char *actual_md5, const u_char *expect_md5, int strict)
{
    struct mem_cache_private *const priv = s3b->data;
    struct mem_cache_conf *const config = priv->config;
    s3b_block_t mem_block_num;
    size_t fragoff;
    u_int multiple;
    int r;

    assert(expect_md5 == NULL);
    assert(actual_md5 == NULL);

    /* Grab lock */
	pthread_mutex_lock(&priv->pri_mutex);

	multiple = config->mem_block_size / config->block_size;
    mem_block_num = block_num / multiple;
    fragoff = (block_num % multiple) * (size_t)config->block_size;
	(*config->log)(LOG_DEBUG, "mem_cache_read_block >>>>>>>>>>> mem_block_num=%u fragoff=0x%jx",
			mem_block_num,(uintmax_t)fragoff);
	//从大块读取小块到dest，注：读取长度为磁盘缓存块大小
	r = mem_cache_read(priv, mem_block_num, fragoff, config->block_size, dest);

	/* Release lock */
	pthread_mutex_unlock(&priv->pri_mutex);
	return r;
//    return (*priv->inner->read_block)(priv->inner,block_num,dest,actual_md5,expect_md5,strict);
}

/*
 * Read a block or a portion thereof.
 *
 * Assumes the mutex is held.
 */
static int
mem_cache_read(struct mem_cache_private *const priv, s3b_block_t block_num, u_int off, u_int len, void *dest)
{
	struct mem_cache_conf *const config = priv->config;
	struct mem_entry *entry;
	int r;
	/* Sanity check */
	assert(off <= config->mem_block_size);
	assert(len <= config->mem_block_size);
	assert(off + len <= config->mem_block_size);

again:
	/* a mem_entry already exists */
	if ((entry = s3b_hash_get(priv->hashtable, block_num)) != NULL) {
		assert(entry->block_num == block_num);
		switch (ENTRY_GET_STATE(entry)) {
		case READING:       /* Wait for other thread already reading this block to finish */
			pthread_cond_wait(&priv->pri_end_reading, &priv->pri_mutex);
			goto again;
		case CLEAN:
		case DIRTY:
		case WRITING:
			memcpy((char *)dest, (char *)entry->buf + off, len);
//			pthread_mutex_lock(&entry->mem_mutex);
			entry->update_time = mem_cache_get_time_millis();
//			pthread_mutex_unlock(&entry->mem_mutex);
			break;
		default:
			assert(0);
			break;
		}
		return 0;
	}
	/* 新建mem_entry及数据buffer */
	if ((r = mem_cache_get_entry(priv, &entry)) != 0) {
		pthread_cond_wait(&priv->pri_space_avail, &priv->pri_mutex);
		goto again;
	}
	if ((r = mem_cache_read_inner(priv,block_num,entry)) != 0) {
		(*config->log)(LOG_ERR, "error read mem_entry from cache_entry! %s", strerror(r));
		return r;
	}
	goto again;
}

/*
 * 从下一层读取memory cache block数据并加入队列
 */
static int
mem_cache_read_inner(struct mem_cache_private *const priv, s3b_block_t mem_block_num, struct mem_entry *entry)
{
	struct mem_cache_conf *const config = priv->config;
	const u_int block_bits = ffs(config->block_size) - 1;
	char *buf;
	u_int num_blocks = config->mem_block_size >> block_bits;
	u_int min_nums;
	u_int offs = 0;
	int i;
	int r;

	/* 初始化mem_entry, 状态:reading */
//	pthread_mutex_lock(&priv->pri_mutex);
	entry->block_num = mem_block_num;
	entry->state = READING;
	ENTRY_RESET_LINK(entry);
	s3b_hash_put_new(priv->hashtable, entry);
	TAILQ_INSERT_TAIL(&priv->queue, entry, link);
//	pthread_mutex_unlock(&priv->pri_mutex);
	assert(ENTRY_GET_STATE(entry) == READING);

	/* 计算磁盘缓存层block_num */
	s3b_block_t cache_block_num = mem_block_num * num_blocks;

	/* 从磁盘缓存层读取数据,并写入mem_entry(可能包含多个cache_entry)*/
    while (num_blocks-- > 0) {
    	if ((buf = malloc(config->block_size)) == NULL) {
    		r = errno;
			(*config->log)(LOG_ERR, "can't allocate block cache buffer: %s", strerror(r));
			return r;
    	}
    	pthread_mutex_unlock(&priv->pri_mutex);
        r = (*priv->inner->read_block)(priv->inner, cache_block_num, buf, NULL, NULL, 0);
        pthread_mutex_lock(&priv->pri_mutex);
        /* 写入buf到mem_entry中数据buffer */
        if ((r = get_mem_cache_data(priv, entry, buf, offs, config->block_size)) != 0)
			(*config->log)(LOG_ERR, "error updating mem_cache block! %s", strerror(r));
        offs += config->block_size;
        cache_block_num++;
//        (*config->log)(LOG_DEBUG, ">>>>>>>>>>>>>>> buf=%s", buf);
        free(buf);
    }

    assert(s3b_hash_get(priv->hashtable, mem_block_num) == entry);
	assert(ENTRY_GET_STATE(entry) == READING);
	pthread_cond_broadcast(&priv->pri_end_reading);
	pthread_cond_signal(&priv->pri_space_avail);
    /* 更新min_entry，状态为clean, 注：偏移量为相对的*/
//    pthread_mutex_lock(&entry->mem_mutex);
    entry->state = CLEAN;
    min_nums = ceil((double)config->mem_block_size / config->min_block_size);
    for (i = 0; i < min_nums; i++) {
    	(*entry->array_min[i]).offset = i * config->min_block_size;
    	(*entry->array_min[i]).size = config->min_block_size;
    	(*entry->array_min[i]).state = CLEAN;
    }
	entry->update_time = mem_cache_get_time_millis();
//	pthread_mutex_unlock(&entry->mem_mutex);
	return 0;
}

static int
mem_cache_get_entry(struct mem_cache_private *priv, struct mem_entry **entryp)
{
    struct mem_cache_conf *const config = priv->config;
    struct mem_entry *entry;
    struct min_block *min_entry;
    void *data = NULL;
    u_int min_entry_size;
    u_int hash_length;
    u_int multiple;
    int r;

    multiple = config->mem_block_size / config->block_size;
    hash_length = ceil((double)config->cache_size / multiple);
	min_entry_size = ceil((double)config->mem_block_size / config->min_block_size);

	if (s3b_hash_size(priv->hashtable) < hash_length) {
        if ((entry = calloc(1, sizeof(*entry))) == NULL) {
            r = errno;
            (*config->log)(LOG_ERR, "can't allocate mem_entry memory entry: %s", strerror(r));
            return r;
        }
    } else
        goto done;

    /* Get data buffer */
	if ((data = malloc(config->mem_block_size)) == NULL) {
		r = errno;
		(*config->log)(LOG_ERR, "can't allocate data memory buffer: %s", strerror(r));
		free(entry);
		return r;
	}
	entry->buf = data;

	void *dd = NULL;
	if ((dd = malloc(sizeof(*min_entry) * min_entry_size)) == NULL) {
		r = errno;
		(*config->log)(LOG_ERR, "can't allocate min_entry memory buffer: %s", strerror(r));
		free(entry);
		free(data);
		return r;
	}
	entry->array_min = dd;

done:
    *entryp = entry;
    return 0;
}

static int
mem_cache_meta_data(struct s3backer_store *s3b, off_t *file_sizep, u_int *block_sizep)
{
    struct mem_cache_private *const priv = s3b->data;

    return (*priv->inner->meta_data)(priv->inner, file_sizep, block_sizep);
}

static int
mem_cache_set_mounted(struct s3backer_store *s3b, int *old_valuep, int new_value)
{
	struct mem_cache_private *const priv = s3b->data;

	return (*priv->inner->set_mounted)(priv->inner, old_valuep, new_value);
}

static int
mem_cache_flush(struct s3backer_store *const s3b)
{
    struct mem_cache_private *const priv = s3b->data;
    struct mem_cache_conf *const config = priv->config;
    /* Grab lock and sanity check */
    pthread_mutex_lock(&priv->pri_mutex);
    S3BCACHE_CHECK_INVARIANTS(priv);

    /* Wait for all blocks to timeout or all worker threads to exit */
    priv->stopping = 1;
    while (TAILQ_FIRST(&priv->queue) != NULL || priv->num_threads > 0) {
        pthread_cond_broadcast(&priv->pri_worker_work);
        pthread_cond_wait(&priv->pri_worker_exit, &priv->pri_mutex);
        (*config->log)(LOG_ERR, ">>>mem_cache_flush----");
    }

    /* Release lock */
    pthread_mutex_unlock(&priv->pri_mutex);
    return 0;
}

static void
mem_cache_destroy(struct s3backer_store *const s3b)
{
    struct mem_cache_private *const priv = s3b->data;
    struct mem_cache_conf *const config = priv->config;

    /* Grab lock and sanity check */
    pthread_mutex_lock(&priv->pri_mutex);
    S3BCACHE_CHECK_INVARIANTS(priv);

    /* Wait for all blocks to timeout or all worker threads to exit */
    priv->stopping = 1;
    while (TAILQ_FIRST(&priv->queue) != NULL || priv->num_threads > 0) {
        pthread_cond_broadcast(&priv->pri_worker_work);
        pthread_cond_wait(&priv->pri_worker_exit, &priv->pri_mutex);
        (*config->log)(LOG_ERR, ">>>mem_cache_destroy----");
    }

    /* Destroy inner store */
    (*priv->inner->destroy)(priv->inner);

    /* Free structures */
    s3b_hash_foreach(priv->hashtable, mem_cache_free_one, priv);
    s3b_hash_destroy(priv->hashtable);
	pthread_cond_destroy(&priv->pri_space_avail);
    pthread_cond_destroy(&priv->pri_end_reading);
    pthread_cond_destroy(&priv->pri_write_complete);
    pthread_cond_destroy(&priv->pri_worker_exit);
    pthread_cond_destroy(&priv->pri_worker_work);
    pthread_mutex_destroy(&priv->pri_mutex);
    free(priv);
    free(s3b);
}
static int
mem_cache_list_blocks(struct s3backer_store *s3b, block_list_func_t *callback, void *arg)
{
    struct mem_cache_private *const priv = s3b->data;
    return (*priv->inner->list_blocks)(priv->inner, callback, arg);
/*    struct cbinfo cbinfo;
    int r;

    if ((r = (*priv->inner->list_blocks)(priv->inner, callback, arg)) != 0)
        return r;
    cbinfo.callback = callback;
    cbinfo.arg = arg;
    pthread_mutex_lock(&priv->pri_mutex);
    s3b_hash_foreach(priv->hashtable, block_cache_dirty_callback, &cbinfo);
    pthread_mutex_unlock(&priv->pri_mutex);
    return 0;*/
}

/*
 * Write the data to a buffer.
 */
static int
get_mem_cache_data(struct mem_cache_private *priv, struct mem_entry *entry, const void *src, u_int off, u_int len)
{
    struct mem_cache_conf *const config = priv->config;
    /* Sanity check */
    assert(off <= config->mem_block_size);
    assert(len <= config->mem_block_size);
    assert(off + len <= config->mem_block_size);
    if (src == NULL)    //删除文件时，由fuse_op_fallocate触发
		memset((char *)entry->buf + off, 0, len);
	else
		memcpy((char *)entry->buf + off, (char *)src, len);
	return 0;
}

/*
 * 当队列为空或者超时，线程进入休眠状态
 * 注：超时计算时，单位换算为uint64_t
 * 注：配合互斥锁使用
 */
static void
mem_cache_worker_wait(struct mem_cache_private *priv, struct mem_entry *entry)
{
	struct mem_cache_conf *const config = priv->config;
    uint64_t wake_time_millis;
    struct timespec wake_time;

    if (entry == NULL) {
        pthread_cond_wait(&priv->pri_worker_work, &priv->pri_mutex);
        return;
    }
    wake_time_millis = ((uint64_t)config->mem_timeout) * TIME_UNIT_MILLIS + entry->update_time;
    wake_time.tv_sec = wake_time_millis / 1000;
    wake_time.tv_nsec = (wake_time_millis % 1000) * 1000000;
    pthread_cond_timedwait(&priv->pri_worker_work, &priv->pri_mutex, &wake_time);
}

/*
 * Return current time in units of TIME_UNIT_MILLIS milliseconds since startup.
 */
static uint32_t
mem_cache_get_time(struct mem_cache_private *priv)
{
    uint64_t timer;

    timer = mem_cache_get_time_millis();
    return (uint32_t)(timer / TIME_UNIT_MILLIS);
}

/*
 * Return current time in milliseconds.
 */
static uint64_t
mem_cache_get_time_millis(void)
{
//	uint64_t since_start;
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + (uint64_t)tv.tv_usec / 1000;
//    return (uint32_t)(since_start / TIME_UNIT_MILLIS);
}

static void
mem_cache_free_one(void *arg, void *value)
{
    struct mem_cache_private *const priv = arg;
    struct mem_cache_conf *const config = priv->config;
    struct mem_entry *const entry = value;

    free(entry->buf);
    free(entry);
}

/*
 * 计算脏块占比
 */
static double
mem_cache_dirty_ratio(struct mem_cache_private *priv, struct mem_entry *entry)
{
    struct mem_cache_conf *const config = priv->config;
    u_int min_entry_size;
	min_entry_size = ceil((double)config->mem_block_size / config->min_block_size);

    return (double)entry->num_dirties / (double)min_entry_size;
}


