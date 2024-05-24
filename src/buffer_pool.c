#include "buffer_pool.h"
#include "file_io.h"

#include <stdio.h>
#include <stdlib.h>

/*typedef struct {
  FileInfo file;
  Page pages[CACHE_PAGE];
  off_t addrs[CACHE_PAGE];
  size_t cnt[CACHE_PAGE];
  size_t ref[CACHE_PAGE];
} BufferPool;*/
void init_buffer_pool(const char *filename, BufferPool *pool)
{
    FileIOResult res = open_file(&pool->file, filename);
    assert(res == FILE_IO_SUCCESS);
    memset(pool->addrs, -1, sizeof(pool->addrs));
    memset(pool->cnt, -1, sizeof(pool->cnt));
    memset(pool->ref, 0, sizeof(pool->ref));
}

void close_buffer_pool(BufferPool *pool)
{
    for (int i = 0; i < CACHE_PAGE; ++i)
    {
        if (pool->addrs[i] != -1)
        {
            write_page(&pool->pages[i], &pool->file, pool->addrs[i]);
        }
    }
    close_file(&pool->file);
}

Page *get_page(BufferPool *pool, off_t addr)
{
    for (int i = 0; i < CACHE_PAGE; ++i) // Find the page in the buffer pool
    {
        if (pool->addrs[i] == addr)
        {
            pool->ref[i]++;
            return &pool->pages[i];
        }
    }
    int idx = -1;
    for (int i = 0; i < CACHE_PAGE; ++i) // Find an empty slot in the buffer pool
    {
        if (pool->addrs[i] == -1)
        {
            idx = i;
            break;
        }
    }
    if (idx == -1) // Find the least recently used page in the buffer pool
    {
        idx = 0;
        for (int i = 1; i < CACHE_PAGE; ++i)
        {
            if (pool->cnt[i] < pool->cnt[idx])
            {
                idx = i;
            }
        }
    }
    if (pool->cnt[idx] != -1)
    {
        write_page(&pool->pages[idx], &pool->file, pool->addrs[idx]);
    }
    read_page(&pool->pages[idx], &pool->file, addr);
    pool->addrs[idx] = addr;
    pool->cnt[idx] = 0;
    pool->ref[idx] = 1;
    return &pool->pages[idx];
}

void release(BufferPool *pool, off_t addr)
{
    for (int i = 0; i < CACHE_PAGE; ++i)
    {
        if (pool->addrs[i] == addr)
        {
            pool->cnt[i]++;
            pool->ref[i]--;
            break;
        }
    }
}

/* void print_buffer_pool(BufferPool *pool) {
} */

/* void validate_buffer_pool(BufferPool *pool) {
} */
