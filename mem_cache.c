
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
	struct min_block       *array_min[1];  // array of min_block
    TAILQ_ENTRY(mem_entry) link;           // next in list (cleans or dirties)
    u_int                  num_dirties;    // min_block's number that are dirty
    u_int64_t              update_time;    // last update time
    u_char                 *buf;           // buffer in memroy
    pthread_mutex_t        mem_mutex;      // my mutex
};

/*针对mem_entry的链表宏定义*/
#define ENTRY_IN_LIST(entry)                ((entry)->link.tqe_prev != NULL)
#define ENTRY_RESET_LINK(entry)             do { (entry)->link.tqe_prev = NULL; } while (0)
#define ENTRY_GET_STATE(entry)              ((entry)->state == 0 ?                       \
		                                         CLEAN : ((entry)->state == 1 ?           \
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
    pthread_cond_t                  space_avail;    // there is new space available in cache
    pthread_cond_t                  write_complete; // a write has completed
    pthread_cond_t                  end_reading;    // some entry in state READING changed state
    pthread_cond_t                  worker_work;    // there is new work for worker thread(s)
    pthread_cond_t                  worker_exit;    // a worker thread has exited
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
    int hash_length;

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
    if ((r = pthread_cond_init(&priv->space_avail, NULL)) != 0)
		goto fail3;
	if ((r = pthread_cond_init(&priv->end_reading, NULL)) != 0)
		goto fail4;
	if ((r = pthread_cond_init(&priv->worker_work, NULL)) != 0)
		goto fail5;
	if ((r = pthread_cond_init(&priv->worker_exit, NULL)) != 0)
		goto fail6;
	if ((r = pthread_cond_init(&priv->write_complete, NULL)) != 0)
		goto fail7;
    TAILQ_INIT(&priv->queue);

    /* Create hash table */
    int cacheSize = config->block_size * config->cache_size;
    hash_length = ceil(cacheSize /  config->mem_block_size);
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
        pthread_cond_broadcast(&priv->worker_work);
        pthread_cond_wait(&priv->worker_exit, &priv->pri_mutex);
    }
	while ((entry = TAILQ_FIRST(&priv->queue)) != NULL) {
		TAILQ_REMOVE(&priv->queue, entry, link);
		free(entry);
	}
    s3b_hash_destroy(priv->hashtable);
fail8:
    pthread_cond_destroy(&priv->write_complete);
fail7:
    pthread_cond_destroy(&priv->worker_exit);
fail6:
    pthread_cond_destroy(&priv->worker_work);
fail5:
    pthread_cond_destroy(&priv->end_reading);
fail4:
    pthread_cond_destroy(&priv->space_avail);
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

	pthread_mutex_lock(&priv->pri_mutex);
	pthread_cond_signal(&priv->space_avail);
	pthread_cond_signal(&priv->worker_work);
	pthread_cond_signal(&priv->worker_exit);
	(*config->log)(LOG_DEBUG, ".....................");
	pthread_mutex_unlock(&priv->pri_mutex);
	return NULL;
}

static int
mem_cache_write_block(struct s3backer_store *const s3b, s3b_block_t block_num, const void *src, u_char *md5,
  check_cancel_t *check_cancel, void *check_cancel_arg)
{
    struct mem_cache_private *const priv = s3b->data;
    struct mem_cache_conf *const config = priv->config;
    (*config->log)(LOG_DEBUG, "mem_write2>>>>>>>> block_num=%u ",block_num);
    /*struct mem_cache_conf *const config = priv->config;

    assert(md5 == NULL);
    return mem_cache_write(priv, block_num, 0, config->block_size, src);*/
    return (*priv->inner->write_block)(priv->inner,block_num,src,md5,check_cancel,check_cancel_arg);
}

static int
mem_cache_write_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, const void *src)
{
    struct mem_cache_private *const priv = s3b->data;
    struct mem_cache_conf *const config = priv->config;
    size_t size = len;
    s3b_block_t mem_block_num;
    const char *buf = src;
    (*config->log)(LOG_DEBUG, "mem_write1>>>>>>>> block_num=%u off=0x%jx len=0x%jx",block_num,(uintmax_t)off, (uintmax_t)len);

    const u_int block_bits = ffs(config->block_size) - 1;
    off_t offsetPer = block_num << block_bits;
    off_t offset = offsetPer + off;
    (*config->log)(LOG_DEBUG, "mem_write111>>>>>>>>offset=0x%jx ",(uintmax_t)offset);

    const u_int mem_block_bits = ffs(config->mem_block_size) - 1;
    const u_int mask = config->mem_block_size - 1;

    /* Write first block fragment (if any) */
    if ((offset & mask) != 0) {
        size_t fragoff = (size_t)(offset & mask);
        size_t fraglen = (size_t)config->mem_block_size - fragoff;
        if (fraglen > size)
            fraglen = size;
        mem_block_num = offset >> mem_block_bits;
        //此处写入小块到大块，注：大块非起始位置写入
        // mem_block_num   fragoff  fraglen   buf
        (*config->log)(LOG_DEBUG, "mem_write333>>>>>>>> mem_block_num=%u fragoff=0x%jx fraglen=0x%jx",mem_block_num,(uintmax_t)fragoff, (uintmax_t)fraglen);

        buf += fraglen;
        offset += fraglen;
        size -= fraglen;
    }

    mem_block_num = offset >> mem_block_bits;
    /* Write last block fragment (if any) */
	if ((size & mask) != 0) {
		size_t fragoff = 0;
		const size_t fraglen = size & mask;
		//此处写入小块到大块，注：大块为起始位置写入
		// mem_block_num   fragoff  fraglen   buf
		(*config->log)(LOG_DEBUG, "mem_write444>>>>>>>> mem_block_num=%u fragoff=0x%jx fraglen=0x%jx",mem_block_num,(uintmax_t)fragoff, (uintmax_t)fraglen);

	}



    return (*priv->inner->write_block_part)(priv->inner,block_num,off,len,src);
//    return mem_cache_write(priv, block_num, off, len, src);
}

/*
 * Write a block
 */
/*static int
mem_cache_write(struct mem_cache_private *const priv, s3b_block_t block_num, u_int off, u_int len, const void *src)
{
    struct mem_cache_conf *const config = priv->config;
    struct mem_entry *entry;

    (*config->log)(LOG_DEBUG, "mem_cache_write>>>>>>>> %u >>>>offset=0x%jx size=0x%jx",block_num,(uintmax_t)off, (uintmax_t)len);
    return 0;
}*/

static int
mem_cache_read_block_part(struct s3backer_store *s3b, s3b_block_t block_num, u_int off, u_int len, void *dest)
{
    struct mem_cache_private *const priv = s3b->data;
    struct mem_cache_conf *const config = priv->config;
    (*config->log)(LOG_DEBUG, "mem_read1>>>>>>>>block_num=%u offset=0x%jx len=0x%jx ",block_num,(uintmax_t)off, (uintmax_t)len);
//    return mem_cache_read(priv, block_num, off, len, dest);
    return (*priv->inner->read_block_part)(priv->inner,block_num,off,len,dest);
}

static int
mem_cache_read_block(struct s3backer_store *const s3b, s3b_block_t block_num, void *dest,
  u_char *actual_md5, const u_char *expect_md5, int strict)
{
    struct mem_cache_private *const priv = s3b->data;
    struct mem_cache_conf *const config = priv->config;
	(*config->log)(LOG_DEBUG, "mem_read2>>>>>>>>block_num=%u",block_num);
//    struct mem_cache_conf *const config = priv->config;
//    (*config->log)(LOG_DEBUG, "mem_cache_read_block>>>>>>>> %u",block_num);
//	(*config->log)(LOG_DEBUG, "333>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
//    struct mem_cache_conf *const config = priv->config;
    return (*priv->inner->read_block)(priv->inner,block_num,dest,actual_md5,expect_md5,strict);
//    assert(expect_md5 == NULL);
//    assert(actual_md5 == NULL);
//    return mem_cache_read(priv, block_num, 0, config->block_size, dest);
}

/*
 * Read a block or a portion thereof.
 *
 * Assumes the mutex is held.
 */
/*static int
mem_cache_read(struct mem_cache_private *const priv, s3b_block_t block_num, u_int off, u_int len, void *dest)
{
	struct mem_cache_conf *const config = priv->config;
	struct mem_entry *entry;
	(*config->log)(LOG_DEBUG, "mem_cache_do_read>>>>>>>> %u >>>>offset=0x%jx size=0x%jx",block_num,(uintmax_t)off, (uintmax_t)len);
	(*config->log)(LOG_DEBUG, ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
	return 0;
}*/

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

    /* Grab lock and sanity check */
    pthread_mutex_lock(&priv->pri_mutex);
    S3BCACHE_CHECK_INVARIANTS(priv);

    /* Wait for all blocks to timeout or all worker threads to exit */
    priv->stopping = 1;
    while (TAILQ_FIRST(&priv->queue) != NULL || priv->num_threads > 0) {
        pthread_cond_broadcast(&priv->worker_work);
        pthread_cond_wait(&priv->worker_exit, &priv->pri_mutex);
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
        pthread_cond_broadcast(&priv->worker_work);
        pthread_cond_wait(&priv->worker_exit, &priv->pri_mutex);
    }

    /* Destroy inner store */
    (*priv->inner->destroy)(priv->inner);

    /* Free structures */
    s3b_hash_foreach(priv->hashtable, mem_cache_free_one, priv);
    s3b_hash_destroy(priv->hashtable);
    pthread_cond_destroy(&priv->write_complete);
    pthread_cond_destroy(&priv->worker_exit);
    pthread_cond_destroy(&priv->worker_work);
    pthread_cond_destroy(&priv->end_reading);
    pthread_cond_destroy(&priv->space_avail);
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

static void
mem_cache_free_one(void *arg, void *value)
{
    struct mem_cache_private *const priv = arg;
    struct mem_cache_conf *const config = priv->config;
    struct mem_entry *const entry = value;

    free(entry->buf);
    free(entry);
}
