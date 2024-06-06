#include "block.h"
#define get_size(item_id) ((short)get_item_id_size(item_id))
#define get_offset(item_id) ((short)get_item_id_offset(item_id))

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

void init_block(Block* block) {
    block->n_items = 0;
    block->head_ptr = (short)(block->data - (char*)block);
    block->tail_ptr = (short)sizeof(Block);
    memset(block->data, 0, sizeof(block->data));
}

ItemPtr get_item(Block* block, short idx) {
    if (idx < 0 || idx >= block->n_items) {
        printf("get item error: idx is out of range\n");
        return NULL;
    }
    ItemID item_id = get_item_id(block, idx);
    if (get_item_id_availability(item_id)) {
        printf("get item error: item_id is not used\n");
        return NULL;
    }
    return (char*)block + get_offset(item_id);
}

short new_item(Block* block, ItemPtr item, short item_size) {
    if (block->tail_ptr - block->head_ptr < item_size)
        return -1;

    for (short idx = 0; idx < block->n_items; idx++) {//free item_id and space enough
        ItemID item_id = get_item_id(block, idx);
        if (get_item_id_availability(item_id) && get_size(item_id) >= item_size) {
            block->tail_ptr -= item_size;            
            get_item_id(block, idx) = compose_item_id(0, block->tail_ptr, item_size);
            memcpy((char*)block + block->tail_ptr, item, item_size);
            return idx;
        }
    }

    if (block->tail_ptr - block->head_ptr < item_size + sizeof(ItemID))
        return -1;
    
    short idx = block->n_items++;
    block->tail_ptr -= item_size;

    get_item_id(block, idx) = compose_item_id(0, block->tail_ptr, item_size);
    block->head_ptr += sizeof(ItemID);
    memcpy((char*)block + block->tail_ptr, item, item_size);
    return idx;
}

void delete_item(Block* block, short idx) {
    if (idx < 0 || idx > block->n_items)
        return;
        
    ItemID item_id = get_item_id(block, idx);

    if(get_item_id_availability(item_id))
        return;

    short size = get_size(item_id);
    short offset = get_offset(item_id);
    if (size) {
        for (int i = 0; i < block->n_items; i++) {
            ItemID other = get_item_id(block, i);
            if (!get_item_id_availability(other) && get_offset(other) < offset)
                get_item_id(block, i) = compose_item_id(0, get_offset(other) + size, get_size(other)); // modify other item_id
        }
        memmove((char*)block + block->tail_ptr + size, (char*)block + block->tail_ptr, offset - block->tail_ptr);
        block->tail_ptr += size;

        if (idx + 1 != block->n_items) {
            get_item_id(block, idx) = compose_item_id(1, 0, 0);
            return;    
        }
    }
    *(ItemID*)((char*)block + block->head_ptr - sizeof(ItemID)) = 0;
    block->head_ptr -= sizeof(ItemID);
    block->n_items--;
}
