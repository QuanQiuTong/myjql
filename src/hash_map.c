#include "hash_map.h"
#include <stdio.h>
#include <string.h>
#include <stdbool.h>

// Helper functions to avoid code duplication
#define get_block(pool, addr) ((HashMapBlock*)get_page(pool, addr))

static void init_empty_blocks(BufferPool *pool, int num_blocks) {
    for (int i = 0; i < num_blocks; i++) {
        Page *page = get_page(pool, i * PAGE_SIZE);
        release(pool, i * PAGE_SIZE);
    }
}

static void init_directory_block(BufferPool *pool, off_t offset) {
    HashMapDirectoryBlock *dir_block = (HashMapDirectoryBlock *)get_page(pool, offset);
    memset(dir_block->directory, 0, sizeof(dir_block->directory));
    release(pool, offset);
}

static void initialize_hash_map_block(BufferPool *pool, off_t offset) {
    HashMapBlock *block = get_block(pool, offset);
    block->next = 0;
    block->n_items = 0;
    memset(block->table, -1, sizeof(block->table));
    release(pool, offset);
}

void hash_table_init(const char *filename, BufferPool *pool, off_t n_directory_blocks) {
    init_buffer_pool(filename, pool);

    if (pool->file.length) return;

    init_empty_blocks(pool, (int)n_directory_blocks + 2);

    HashMapControlBlock *ctrl = (HashMapControlBlock *)get_page(pool, 0);
    ctrl->free_block_head = (n_directory_blocks + 1) * PAGE_SIZE;
    ctrl->n_directory_blocks = n_directory_blocks;
    ctrl->max_size = n_directory_blocks * HASH_MAP_DIR_BLOCK_SIZE;
    ctrl->n_block = 2 + n_directory_blocks;

    for (int i = 1; i <= n_directory_blocks; i++) {
        init_directory_block(pool, i * PAGE_SIZE);
    }

    initialize_hash_map_block(pool, ctrl->free_block_head);
    release(pool, 0);
}

void hash_table_close(BufferPool *pool) {
    close_buffer_pool(pool);
}

static off_t free_block(BufferPool *pool, off_t free_addr, off_t addr) {
    HashMapBlock *free_block = get_block(pool, free_addr);
    off_t next = free_block->next;
    free_block->n_items = 1;
    free_block->next = 0;
    free_block->table[0] = addr;
    release(pool, free_addr);
    return next;
}

static void end_block(BufferPool *pool, off_t hash_end, off_t addr) {
    HashMapBlock *block = get_block(pool, hash_end); // target_block_next
    block->n_items = 1;
    block->next = 0;
    block->table[0] = addr;
    memset(block->table + 1, -1, sizeof(block->table)- sizeof(*(block->table)));
    release(pool, hash_end);
}

void hash_table_insert(BufferPool *pool, short size, off_t addr) {
    HashMapControlBlock *ctrl = (HashMapControlBlock *)get_page(pool, 0);
    if (size >= ctrl->max_size) return release(pool, 0);

    short dir_id = size / HASH_MAP_DIR_BLOCK_SIZE;
    short block_id = size % HASH_MAP_DIR_BLOCK_SIZE;
    HashMapDirectoryBlock *target_dir = (HashMapDirectoryBlock *)get_page(pool, (dir_id + 1) * PAGE_SIZE);

    off_t target_block_addr = target_dir->directory[block_id];
    HashMapBlock *target_block;

    if (target_block_addr) {
        bool is_duplicate = false;
        target_block = get_block(pool, target_block_addr);

        while (target_block->n_items >= HASH_MAP_BLOCK_SIZE && target_block->next) {
            for (int i = 0; i < HASH_MAP_BLOCK_SIZE; i++) {
                if (addr == target_block->table[i]) is_duplicate = true;
            }
            release(pool, target_block_addr);
            target_block_addr = target_block->next;
            target_block = get_block(pool, target_block_addr);
        }

        if (is_duplicate) return release(pool, target_block_addr), release(pool, (dir_id + 1) * PAGE_SIZE), release(pool, 0);

        if (target_block->n_items < HASH_MAP_BLOCK_SIZE) {
            for (int i = 0; i < HASH_MAP_BLOCK_SIZE; i++) {
                if (target_block->table[i] == -1) {
                    target_block->table[i] = addr;
                    target_block->n_items++;
                    break;
                }
            }
        } else {
            off_t free_addr = ctrl->free_block_head;
            if (free_addr) {
                target_block->next = free_addr;
                ctrl->free_block_head = free_block(pool, free_addr, addr);
            } else {
                off_t hash_end = ctrl->n_block * PAGE_SIZE;
                target_block->next = hash_end;
                ctrl->n_block++;
                end_block(pool, hash_end, addr);
            }
        }
        release(pool, target_block_addr);
    } else {
        off_t free_addr = ctrl->free_block_head;
        if (free_addr) {
            target_dir->directory[block_id] = free_addr;
            ctrl->free_block_head = free_block(pool, free_addr, addr);
        } else {
            off_t hash_end = ctrl->n_block * PAGE_SIZE;
            target_dir->directory[block_id] = hash_end;
            ctrl->n_block++;
            end_block(pool, hash_end, addr);
        }
    }
    release(pool, (dir_id + 1) * PAGE_SIZE);
    release(pool, 0);
}

off_t hash_table_pop_lower_bound(BufferPool *pool, short size) {
    HashMapControlBlock *ctrl = (HashMapControlBlock *)get_page(pool, 0);
    for (; size < ctrl->max_size; ++size) {
        short dir_id = size / HASH_MAP_DIR_BLOCK_SIZE;
        short block_id = size % HASH_MAP_DIR_BLOCK_SIZE;
        HashMapDirectoryBlock *target_dir = (HashMapDirectoryBlock *)get_page(pool, (dir_id + 1) * PAGE_SIZE);

        if (target_dir->directory[block_id]) {
            off_t block_addr = target_dir->directory[block_id];
            HashMapBlock *block = get_block(pool, block_addr);
            release(pool, block_addr);

            while (block_addr) {
                block = get_block(pool, block_addr);
                off_t next_addr = block->next;
                release(pool, block_addr);
                block_addr = next_addr;
            }

            if (block->n_items == HASH_MAP_BLOCK_SIZE) {
                off_t addr = block->table[HASH_MAP_BLOCK_SIZE - 1];
                hash_table_pop(pool, size, addr);
                release(pool, (dir_id + 1) * PAGE_SIZE);
                release(pool, 0);
                return addr;
            }
            for (int i = 0; i <= block->n_items; i++) {
                if (block->table[i] == -1) {
                    off_t addr = block->table[i - 1];
                    hash_table_pop(pool, size, addr);
                    release(pool, (dir_id + 1) * PAGE_SIZE);
                    release(pool, 0);
                    return addr;
                }
            }
        }
        release(pool, (dir_id + 1) * PAGE_SIZE);
    }
    release(pool, 0);
    return -1;
}

void hash_table_pop(BufferPool *pool, short size, off_t addr) {
    HashMapControlBlock *ctrl = (HashMapControlBlock *)get_page(pool, 0);
    HashMapDirectoryBlock *dir_block = (HashMapDirectoryBlock *)get_page(pool, (size / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);

    off_t block_addr = dir_block->directory[size % HASH_MAP_DIR_BLOCK_SIZE];
    if (!block_addr) return release(pool, (size / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE), release(pool, 0);

    HashMapBlock *block = get_block(pool, block_addr);
    if (block->n_items == 1) {
        block->table[0] = -1;
        block->n_items--;
        dir_block->directory[size % HASH_MAP_DIR_BLOCK_SIZE] = 0;

        block->next = ctrl->free_block_head;
        ctrl->free_block_head = block_addr;
        goto ret;
    }

    for (off_t next_addr, last_addr; block_addr; last_addr = block_addr, block_addr = next_addr) {
        block = get_block(pool, block_addr);
        next_addr = block->next;
        for (int j = 0; j < block->n_items; ++j) {
            if (block->table[j] == addr) {
                block->table[j] = -1;
                if (j != block->n_items - 1) {
                    block->table[j] = block->table[block->n_items - 1];
                    block->table[block->n_items - 1] = -1;
                }
                block->n_items--;
                if (block->n_items == 0) {
                    get_block(pool, last_addr)->next = next_addr;
                    block->next = ctrl->free_block_head;
                    ctrl->free_block_head = block_addr;
                    release(pool, last_addr);
                }
                goto ret;
            }
        }
        release(pool, block_addr);
    }
ret:
    release(pool, block_addr);
    release(pool, (size / HASH_MAP_DIR_BLOCK_SIZE + 1) * PAGE_SIZE);
    release(pool, 0);
}

void print_hash_table(BufferPool *pool) {
}