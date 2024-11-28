#include "buffer_pool.h"
#include "file_io.h"

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

/*typedef struct {
  FileInfo file;
  Page pages[CACHE_PAGE];
  off_t addrs[CACHE_PAGE];
  size_t cnt[CACHE_PAGE];
  size_t ref[CACHE_PAGE];
  bool avail[CACHE_PAGE];
} BufferPool;*/

void init_buffer_pool(const char *filename, BufferPool *pool)
{
    open_file(&pool->file, filename);
    for (int i = 0; i < CACHE_PAGE; i++)
    {
        pool->addrs[i] = -1;
        pool->cnt[i] = 0;
        pool->avail[i] = true;
        read_page(&pool->pages[i], &pool->file, PAGE_SIZE * i);
    }
}

void close_buffer_pool(BufferPool *pool)
{
    for (int i = 0; i < CACHE_PAGE; i++)
        write_page(&pool->pages[i], &pool->file, pool->addrs[i]);
    close_file(&pool->file);
}

Page *get_page(BufferPool *pool, off_t addr) // LRU
{
    bool not_empty = false;
    for (int i = 0; i < CACHE_PAGE; i++)
    {
        if (pool->addrs[i] == addr)
        {
            pool->cnt[i] = 0;
            pool->avail[i] = false;
            return &pool->pages[i];
        }
        pool->cnt[i]++;
        not_empty |= pool->avail[i];
    }

    if (not_empty)
        for (int i = 0; i < CACHE_PAGE; i++)
            if (pool->avail[i])
            {
                read_page(&pool->pages[i], &pool->file, addr);
                pool->addrs[i] = addr;
                pool->cnt[i] = 0;
                pool->avail[i] = false;
                return &pool->pages[i];
            }

    size_t max_cnt = 0;
    int target = -1;
    for (int i = 0; i < CACHE_PAGE; i++)
    {
        if (pool->cnt[i] > max_cnt)
        {
            max_cnt = pool->cnt[i];
            target = i;
        }
    }
    release(pool, pool->addrs[target]);
    pool->addrs[target] = addr;
    pool->cnt[target] = 0;
    pool->avail[target] = false;
    return &pool->pages[target];
}

void release(BufferPool *pool, off_t addr)
{
    for (int i = 0; i < CACHE_PAGE; i++)
        if (pool->addrs[i] == addr)
        {
            write_page(&pool->pages[i], &pool->file, pool->addrs[i]);
            pool->addrs[i] = -1;
            pool->avail[i] = true;
            return;
        }
}

/* void print_buffer_pool(BufferPool *pool) {
} */

/* void validate_buffer_pool(BufferPool *pool) {
} */
