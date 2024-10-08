#include "block.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

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
    return (char*)block + get_item_id_offset(item_id);
}

short new_item(Block* block, ItemPtr item, short item_size) {
    if (block->tail_ptr - block->head_ptr < item_size)
        return -1;

    for (short idx = 0; idx < block->n_items; idx++) {
        ItemID item_id = get_item_id(block, idx);
        if (get_item_id_availability(item_id) && get_item_id_size(item_id) >= item_size) {
            block->tail_ptr -= item_size;
            get_item_id(block, idx) = compose_item_id(0, block->tail_ptr, item_size);
            memcpy(block->data + block->tail_ptr - 3 * sizeof(short), item, item_size);
            return idx;
        }
    }

    if (block->tail_ptr - block->head_ptr < item_size + sizeof(ItemID))
        return -1;

    short idx = block->n_items++;
    block->tail_ptr -= item_size;
    
    get_item_id(block, idx) = compose_item_id(0, block->tail_ptr, item_size);
    block->head_ptr += sizeof(ItemID);
    memcpy(block->data + block->tail_ptr - 3 * sizeof(short), item, item_size);
    return idx;
}

void delete_item(Block* block, short idx) {
    if (idx < 0 || idx >= block->n_items) {
        return;
    }

    ItemID item_id = get_item_id(block, idx);
    short size = get_item_id_size(item_id);
    short offset = get_item_id_offset(item_id);

    if (!get_item_id_availability(item_id) && size != 0) {
        for (int i = 0; i < block->n_items; i++) {
            ItemID other_item_id = get_item_id(block, i);
            if (!get_item_id_availability(other_item_id) && get_item_id_offset(other_item_id) < offset) {
                get_item_id(block, i) = compose_item_id(0, get_item_id_offset(other_item_id) + size, get_item_id_size(other_item_id));
            }
        }

        char tmp_str[PAGE_SIZE - 3 * sizeof(short)];
        short str_length = (short)((char*)block + block->tail_ptr - (char*)get_item(block, idx));
        memcpy(tmp_str, block->data + block->tail_ptr - 3 * sizeof(short), str_length);

        memset(block->data + block->tail_ptr - 3 * sizeof(short), 0, size);
        block->tail_ptr += size;
        memcpy(block->data + block->tail_ptr - 3 * sizeof(short), tmp_str, str_length);

        if (idx + 1 == block->n_items) {
            memset(block->data + block->head_ptr - 3 * sizeof(short) - sizeof(ItemID), 0, sizeof(ItemID));
            block->head_ptr -= sizeof(ItemID);
            block->n_items--;
        } else {
            get_item_id(block, idx) = compose_item_id(1, 0, 0);
        }
    } else if (!get_item_id_availability(item_id) && size == 0) {
        memset(block->data + block->head_ptr - 3 * sizeof(short) - sizeof(ItemID), 0, sizeof(ItemID));
        block->head_ptr -= sizeof(ItemID);
        block->n_items--;
    }
}


/* void str_printer(ItemPtr item, short item_size) {
    if (item == NULL) {
        printf("NULL");
        return;
    }
    short i;
    printf("\"");
    for (i = 0; i < item_size; ++i) {
        printf("%c", item[i]);
    }
    printf("\"");
}

void print_block(Block *block, printer_t printer) {
    short i, availability, offset, size;
    ItemID item_id;
    ItemPtr item;
    printf("----------BLOCK----------\n");
    printf("total = %d\n", block->n_items);
    printf("head = %d\n", block->head_ptr);
    printf("tail = %d\n", block->tail_ptr);
    for (i = 0; i < block->n_items; ++i) {
        item_id = get_item_id(block, i);
        availability = get_item_id_availability(item_id);
        offset = get_item_id_offset(item_id);
        size = get_item_id_size(item_id);
        if (!availability) {
            item = get_item(block, i);
        } else {
            item = NULL;
        }
        printf("%10d%5d%10d%10d\t", i, availability, offset, size);
        printer(item, size);
        printf("\n");
    }
    printf("-------------------------\n");
}

void analyze_block(Block *block, block_stat_t *stat) {
    short i;
    stat->empty_item_ids = 0;
    stat->total_item_ids = block->n_items;
    for (i = 0; i < block->n_items; ++i) {
        if (get_item_id_availability(get_item_id(block, i))) {
            ++stat->empty_item_ids;
        }
    }
    stat->available_space = block->tail_ptr - block->head_ptr 
        + stat->empty_item_ids * sizeof(ItemID);
}

void accumulate_stat_info(block_stat_t *stat, const block_stat_t *stat2) {
    stat->empty_item_ids += stat2->empty_item_ids;
    stat->total_item_ids += stat2->total_item_ids;
    stat->available_space += stat2->available_space;
}

void print_stat_info(const block_stat_t *stat) {
    printf("==========STAT==========\n");
    printf("empty_item_ids: " FORMAT_SIZE_T "\n", stat->empty_item_ids);
    printf("total_item_ids: " FORMAT_SIZE_T "\n", stat->total_item_ids);
    printf("available_space: " FORMAT_SIZE_T "\n", stat->available_space);
    printf("========================\n");
} */