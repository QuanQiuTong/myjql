#include"str.h"

#include "table.h"

#include<string.h>

#include<stdlib.h>

void read_string(Table* table, RID rid, StringRecord* record) {
    table_read(table, rid, (ItemPtr)&record->chunk);
    record->idx = 0;
}

int has_next_char(StringRecord* record) {
    return (get_str_chunk_size(&(record->chunk)) != record->idx) ||
           (get_rid_idx(get_str_chunk_rid(&(record->chunk))) != -1);
}

char next_char(Table* table, StringRecord* record) {
    StringChunk* chunk = &record->chunk;
    if (record->idx != get_str_chunk_size(chunk) - sizeof(RID) - sizeof(short) - 1)
        return get_str_chunk_data_ptr(chunk)[++record->idx];
    
    RID rid = get_str_chunk_rid(chunk);
    off_t rid_addr = get_rid_block_addr(rid);
    short rid_idx = get_rid_idx(rid);
    if (rid_addr == -1) //can not find more str
        return '\0';
    Block* block = (Block*)get_page(&table->data_pool, rid_addr);
    release(&table->data_pool, rid_addr);
    StringChunk* next_chunk = (StringChunk*)get_item(block, rid_idx);
    *record = (StringRecord){*next_chunk, 0};
    return get_str_chunk_data_ptr(next_chunk)[0];
}

int compare_string_record(Table* table, const StringRecord* a, const StringRecord* b) {
    StringRecord rec_a = *a, rec_b = *b;
    rec_a.idx = rec_b.idx = 0;
    char char_a = get_str_chunk_data_ptr(&rec_a.chunk)[rec_a.idx++];
    char char_b = get_str_chunk_data_ptr(&rec_b.chunk)[rec_b.idx++];
    
    while (char_a == char_b) {
        if (!has_next_char(&rec_a) && !has_next_char(&rec_b))
            return 0;
        if (!has_next_char(&rec_a))
            return -1;
        if (!has_next_char(&rec_b))
            return 1;
        char_a = next_char(table, &rec_a);
        char_b = next_char(table, &rec_b);
    }
    return (char_a > char_b) - (char_a < char_b);
}

static const RID nil = {-1, -1, -1, -1, -1, -1, -1, -1, 0, 0};
static ItemPtr chunk(RID rid, short size, const char *data) {
    static StringChunk c;
    get_str_chunk_rid(&c) = rid;
    get_str_chunk_size(&c) = calc_str_chunk_size(size);
    memcpy(get_str_chunk_data_ptr(&c), data, size);
    return (ItemPtr)&c;
}

RID write_string(Table* table, const char* data, off_t size) {
    const short mx = STR_CHUNK_MAX_LEN;
    //split string
    off_t cnt = size / mx;
    short endsize = size % mx;
    if (size % 20 == 0) {
        endsize = 20;
        cnt = cnt - 1;
    }

    RID rid = table_insert(table, 
                chunk(nil, endsize, data + cnt * mx), calc_str_chunk_size(endsize));
    while(cnt-- > 1)
        rid = table_insert(table, 
                chunk(rid, mx, data + cnt * mx), STR_CHUNK_MAX_SIZE);
    if (size > mx)
        rid = table_insert(table,
                chunk(rid, mx, data), STR_CHUNK_MAX_SIZE);
    return rid;
}

void delete_string(Table* table, RID rid) {
    off_t addr = get_rid_block_addr(rid);
    short idx = get_rid_idx(rid);
    Block* block = (Block*)get_page(&table->data_pool, addr);
    StringChunk* chunk = (StringChunk*)get_item(block, idx);
    release(&table->data_pool, addr);
    RID next_rid = get_str_chunk_rid(chunk);
    addr = get_rid_block_addr(next_rid);
    idx = get_rid_idx(next_rid);
    RID current_rid = next_rid;
    table_delete(table, rid);
    while (addr != -1) {
        block = (Block*)get_page(&table->data_pool, addr);
        chunk = (StringChunk*)get_item(block, idx);
        next_rid = get_str_chunk_rid(chunk);
        if (addr != -1)
            table_delete(table, current_rid);
        addr = get_rid_block_addr(next_rid);
        idx = get_rid_idx(next_rid);
        current_rid = next_rid;
        release(&table->data_pool, addr);
    }
}

size_t load_string(Table* table, const StringRecord* record, char* dest, size_t max_size) {
    StringRecord rec = *record;
    rec.idx = 0;
    dest[0] = get_str_chunk_data_ptr(&rec.chunk)[0];
    size_t size = 1;
    while (size < max_size && has_next_char(&rec))
        dest[size++] = next_char(table, &rec);
    return size;
}
