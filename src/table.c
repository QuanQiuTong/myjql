#include "table.h"
#include "hash_map.h"
#include <string.h>

void table_init(Table* table, const char* data_filename, const char* fsm_filename) {
    init_buffer_pool(data_filename, &table->data_pool);
    hash_table_init(fsm_filename, &table->fsm_pool, PAGE_SIZE / HASH_MAP_DIR_BLOCK_SIZE);
}

void table_close(Table* table) {
    close_buffer_pool(&table->data_pool);
    hash_table_close(&table->fsm_pool);
}

off_t table_get_total_blocks(Table* table) {
    return table->data_pool.file.length / PAGE_SIZE;
}

#define get_block(pool, addr) ((Block*)get_page(pool, addr))

short table_block_get_total_items(Table* table, off_t block_addr) {
    Block* block = get_block(&table->data_pool, block_addr);
    short n_items = block->n_items;
    release(&table->data_pool, block_addr);
    return n_items;
}

void table_read(Table* table, RID rid, ItemPtr dest) {
    off_t block_addr = get_rid_block_addr(rid);
    short idx = get_rid_idx(rid);
    Block* block = get_block(&table->data_pool, block_addr);

    ItemPtr item = get_item(block, idx);
    short item_size = get_item_id_size(get_item_id(block, idx));

    memcpy(dest, item, item_size);

    release(&table->data_pool, block_addr);
}

static RID compose(off_t addr, short idx) {
    RID rid;
    get_rid_block_addr(rid) = addr;
    get_rid_idx(rid) = idx;
    return rid;
}

RID table_insert(Table* table, ItemPtr src, short size) {
    off_t addr = hash_table_pop_lower_bound(&table->fsm_pool, size);
    if (addr != -1) {
        Block *block = get_block(&table->data_pool, addr);

        short block_size = block->tail_ptr - block->head_ptr;
        short idx = new_item(block, src, size);
        if (idx != -1) {
            hash_table_pop(&table->fsm_pool, block_size, addr);
            hash_table_insert(&table->fsm_pool, block->tail_ptr - block->head_ptr, addr);
            release(&table->data_pool, addr);
            return compose(addr, idx);
        }
        release(&table->data_pool, addr);
    }

    addr = table->data_pool.file.length;
    Block *block = get_block(&table->data_pool, addr);
    init_block(block);
    table->data_pool.file.length += PAGE_SIZE;

    short idx = new_item(block, src, size);
    hash_table_insert(&table->fsm_pool, block->tail_ptr - block->head_ptr, addr);
    release(&table->data_pool, addr);
    return compose(addr, idx);
}

void table_delete(Table* table, RID rid) {
    off_t block_addr = get_rid_block_addr(rid);
    short idx = get_rid_idx(rid);
    Block* block = get_block(&table->data_pool, block_addr);

    short block_size = block->tail_ptr - block->head_ptr;
    delete_item(block, idx);
    short current_size = block->tail_ptr - block->head_ptr;

    hash_table_pop(&table->fsm_pool, block_size, block_addr);
    hash_table_insert(&table->fsm_pool, current_size, block_addr);
    release(&table->data_pool, block_addr);
}

#include <stdio.h>

void print_rid(RID rid) {
    printf("RID(" FORMAT_OFF_T ", %d)", get_rid_block_addr(rid), get_rid_idx(rid));
}
