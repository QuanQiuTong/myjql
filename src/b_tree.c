#include "b_tree.h"
#include "buffer_pool.h"

#include <stdio.h>
#include <string.h>

void b_tree_init(const char *filename, BufferPool *pool) {
    init_buffer_pool(filename, pool);
    if(pool->file.length)
        return;

    BCtrlBlock *ctrlblock = (BCtrlBlock*)get_page(pool,0);
    ctrlblock->root_node = PAGE_SIZE;
    ctrlblock->free_node_head = 2 * PAGE_SIZE;
    ctrlblock->max_size = 16;
    write_page((const Page*)ctrlblock, &pool->file, 0);

    BNode *root = (BNode*)get_page(pool,PAGE_SIZE);
    root->n = 0;
    root->next = -1;
    root->leaf = '1';
    write_page((const Page*)root, &pool->file, PAGE_SIZE);
    release(pool,PAGE_SIZE);

    for(int i=0; i<ctrlblock->max_size; ++i){
        BNode *freeblock = (BNode*)get_page(pool,ctrlblock->free_node_head + PAGE_SIZE*i);
        freeblock->leaf = /*false*/ 0;
        if(i != ctrlblock->max_size-1)
            freeblock->next = ctrlblock->free_node_head + PAGE_SIZE*(i+1);
        else
            freeblock->next = -1;
        write_page((const Page*)freeblock, &pool->file, ctrlblock->free_node_head + PAGE_SIZE*i);
        release(pool,ctrlblock->free_node_head + PAGE_SIZE*i);
    }
    release(pool,0);
}

void b_tree_close(BufferPool *pool) {
    close_buffer_pool(pool);
}

static off_t get_root(BufferPool *pool){
    BCtrlBlock *ctrlblock = (BCtrlBlock*)get_page(pool,0);
    off_t root_off = ctrlblock->root_node;
    release(pool,0);
    return root_off;
}

off_t inside_search(BufferPool *pool, void *key, size_t size, off_t node_off, b_tree_ptr_row_cmp_t cmp){
    BNode *node = (BNode*)get_page(pool,node_off);
    if(node->leaf == '1'){
        release(pool,node_off);
        return node_off;
    } 
    off_t child_off; 
    if(cmp(key, size, node->row_ptr[0]) < 0)
        child_off = node->child[0];
    else if(cmp(key, size, node->row_ptr[node->n-1]) >= 0)
        child_off = node->child[node->n];
    else 
        for(size_t i = 0; i < node->n - 1; ++i)
            if(cmp(key, size, node->row_ptr[i]) >= 0 && cmp(key, size, node->row_ptr[i+1]) < 0){
                child_off = node->child[i+1];
            }
    release(pool,node_off);
    return inside_search(pool,key,size,child_off,cmp);
}

RID b_tree_search(BufferPool *pool, void *key, size_t size, b_tree_ptr_row_cmp_t cmp) {
    off_t root_off = get_root(pool);

    off_t find_node_off = inside_search(pool,key,size,root_off,cmp);
    BNode *find_node = (BNode*)get_page(pool,find_node_off);
    RID ret = nil;
    for(size_t i = 0; i < find_node->n ; ++i) //find the rid of the search value
        if(cmp(key, size, find_node->row_ptr[i]) == 0){
            ret = find_node->row_ptr[i];
            break;
        }
    // didn't find return rid(-1,0);
    release(pool,find_node_off);
    return ret;
}

typedef struct {
    RID newkey_rid;
    off_t node_off;
} newchildentry_t;

static size_t find_i(BNode *nodeptr, RID rid, b_tree_row_row_cmp_t cmp){
    size_t i = 0;
    for(; i < nodeptr->n && cmp(rid, nodeptr->row_ptr[i]) >= 0; ++i)
        ;
    return i;
}

static BNode *new_node(BufferPool *pool, BCtrlBlock *ctrlblock, BNode *nodeptr)
{
    BNode* newNode;
    off_t free_block_off = ctrlblock->free_node_head;
    if (free_block_off != -1)
    {
        newNode = (BNode *)get_page(pool, free_block_off);
        ctrlblock->free_node_head = newNode->next;
    }
    else
    {
        free_block_off = PAGE_SIZE * (ctrlblock->max_size + 2);
        newNode = (BNode *)get_page(pool, free_block_off);
        write_page((const Page *)newNode, &pool->file, free_block_off);
        ++ctrlblock->max_size;
    }
    newNode->next = nodeptr->next;
    nodeptr->next = free_block_off;
    return newNode;
}

static void new_root(BufferPool *pool, BCtrlBlock *ctrlblock, RID new_rid, off_t new_off, off_t node_off, b_tree_insert_nonleaf_handler_t insert_handler)
{
    BNode *newroot;
    off_t free_block_off1 = ctrlblock->free_node_head;
    if (free_block_off1 != -1)
    {
        newroot = (BNode *)get_page(pool, free_block_off1);
        ctrlblock->free_node_head = newroot->next;
    }
    else
    {
        free_block_off1 = PAGE_SIZE * (ctrlblock->max_size + 2);
        newroot = (BNode *)get_page(pool, free_block_off1);
        write_page((const Page *)newroot, &pool->file, free_block_off1);
        ++ctrlblock->max_size;
    }
    ctrlblock->root_node = free_block_off1;
    newroot->n = 1;
    newroot->leaf = '0';
    newroot->next = -1;
    newroot->row_ptr[0] = insert_handler(new_rid);
    newroot->child[0] = node_off;
    newroot->child[1] = new_off;
    release(pool, ctrlblock->root_node);
}

static void ins_leaf(BufferPool *pool, off_t node_off, BNode *node, RID rid, newchildentry_t* newchild, b_tree_row_row_cmp_t cmp, b_tree_insert_nonleaf_handler_t insert_handler)
{
    if (node->n < 2 * DEGREE)
    { // has space
        size_t i = find_i(node, rid, cmp);
        if (i != node->n)
            memmove(node->row_ptr + i + 1, node->row_ptr + i, (node->n - i) * sizeof(RID));
        node->row_ptr[i] = rid;
        ++node->n;
        get_rid_block_addr(newchild->newkey_rid) = -1;
    }
    else
    { // split
        BCtrlBlock *ctrlblock = (BCtrlBlock *)get_page(pool, 0);
        BNode *L2 = new_node(pool, ctrlblock, node);
        L2->leaf = '1';
        L2->n = DEGREE + 1;
        size_t i = find_i(node, rid, cmp);
        if (i < DEGREE)
        {
            memcpy(L2->row_ptr, node->row_ptr + DEGREE - 1, (DEGREE + 1) * sizeof(RID));
            memmove(node->row_ptr + i + 1, node->row_ptr + i, (DEGREE - i - 1) * sizeof(RID));
            node->row_ptr[i] = rid;
        }
        else
        {
            memcpy(L2->row_ptr, node->row_ptr + DEGREE, (i - DEGREE) * sizeof(RID));
            memcpy(L2->row_ptr + i - DEGREE + 1, node->row_ptr + i, (2 * DEGREE - i) * sizeof(RID));
            L2->row_ptr[i - DEGREE] = rid;
        }
        node->n = DEGREE;

        newchild->newkey_rid = L2->row_ptr[0];
        newchild->node_off = node->next;
        release(pool, node->next); // release L2
        if (node_off == ctrlblock->root_node)
            new_root(pool, ctrlblock, newchild->newkey_rid, newchild->node_off, node_off, insert_handler);
        release(pool, 0); // release ctrlblock
    }
}

static void ins_nonleaf(BufferPool *pool, off_t node_off, RID rid, newchildentry_t* newchild, b_tree_row_row_cmp_t cmp, b_tree_insert_nonleaf_handler_t insert_handler)
{
    BNode *node = (BNode *)get_page(pool, node_off); // N
    if (node->n < 2 * DEGREE)
    { // has space
        size_t i = find_i(node, newchild->newkey_rid, cmp);
        if (i != node->n)
        {
            memmove(node->child + i + 2, node->child + i + 1, (node->n - i) * sizeof(off_t));
            memmove(node->row_ptr + i + 1, node->row_ptr + i, (node->n - i) * sizeof(RID));
        }
        node->child[i + 1] = newchild->node_off;
        node->row_ptr[i] = insert_handler(newchild->newkey_rid);
        ++node->n;
        get_rid_block_addr(newchild->newkey_rid) = -1;
        return release(pool, node_off);
    }
    // split
    BCtrlBlock *ctrlblock = (BCtrlBlock *)get_page(pool, 0);
    BNode *N2 = new_node(pool, ctrlblock, node);
    N2->leaf = '0';
    N2->n = DEGREE;
    size_t i = find_i(node, newchild->newkey_rid, cmp);
    if (i == DEGREE)
    {
        memcpy(N2->child + 1, node->child + DEGREE + 1, DEGREE * sizeof(off_t));
        memcpy(N2->row_ptr, node->row_ptr + DEGREE, DEGREE * sizeof(RID));
        N2->child[0] = newchild->node_off;
        newchild->node_off = node->next;
        // newchild->newkey_rid unchanged
    }
    else if (i < DEGREE)
    {
        memcpy(N2->child, node->child + DEGREE, (DEGREE + 1) * sizeof(off_t));
        memcpy(N2->row_ptr, node->row_ptr + DEGREE, DEGREE * sizeof(RID));
        memmove(node->row_ptr + i + 1, node->row_ptr + i, (DEGREE - i) * sizeof(RID));
        memmove(node->child + i + 2, node->child + i + 1, (DEGREE - i - 1) * sizeof(off_t));
        node->child[i + 1] = newchild->node_off;
        node->row_ptr[i] = insert_handler(newchild->newkey_rid);
        newchild->newkey_rid = node->row_ptr[DEGREE];
        newchild->node_off = node->next;
    }
    else
    { // i > DEGREE
        for (int j = 0; j < DEGREE; ++j)
        {
            if (j + DEGREE + 1 < i)
                N2->row_ptr[j] = node->row_ptr[j + DEGREE + 1];
            else if (j + DEGREE + 1 == i)
                N2->row_ptr[j] = insert_handler(newchild->newkey_rid);
            else
                N2->row_ptr[j] = node->row_ptr[j + DEGREE];
        }
        for (int j = 0; j < DEGREE + 1; ++j)
        {
            if (j + DEGREE < i)
                N2->child[j] = node->child[j + DEGREE + 1];
            else if (j + DEGREE == i)
                N2->child[j] = newchild->node_off;
            else
                N2->child[j] = node->child[j + DEGREE];
        }
        newchild->newkey_rid = node->row_ptr[DEGREE];
        newchild->node_off = node->next;
    }
    node->n = DEGREE;

    release(pool, node->next); // release N2
    if (node_off == ctrlblock->root_node)
        new_root(pool, ctrlblock, newchild->newkey_rid ,newchild->node_off, node_off, insert_handler);
    release(pool, 0); // release ctrlblock

    release(pool, node_off);
}

void inside_insert(BufferPool *pool, off_t node_off, RID rid, newchildentry_t* newchild, b_tree_row_row_cmp_t cmp, b_tree_insert_nonleaf_handler_t insert_handler){
    BNode *nodeptr = (BNode*)get_page(pool,node_off);
    if(nodeptr->leaf == '1'){ //leaf node
        ins_leaf(pool,node_off,nodeptr,rid,newchild,cmp,insert_handler);
        release(pool,node_off);
        return;
    }
 //non-leaf node
    off_t child_off;
    if(cmp(rid, nodeptr->row_ptr[0]) < 0){
        child_off = nodeptr->child[0];
    }
    else if(cmp(rid, nodeptr->row_ptr[nodeptr->n-1]) >= 0){
        child_off = nodeptr->child[nodeptr->n];
    }
    else{
        for(int i=0;i<nodeptr->n-1;++i){
            if(cmp(rid, nodeptr->row_ptr[i]) >= 0 && cmp(rid, nodeptr->row_ptr[i+1]) < 0){
                child_off = nodeptr->child[i+1];
                break;
            }
        }
    }
    release(pool,node_off);
    inside_insert(pool,child_off,rid,newchild,cmp,insert_handler);

    if(get_rid_block_addr(newchild->newkey_rid) == -1)
        return;

    ins_nonleaf(pool,node_off,rid,newchild,cmp,insert_handler);
}

RID b_tree_insert(BufferPool *pool, RID rid, b_tree_row_row_cmp_t cmp, b_tree_insert_nonleaf_handler_t insert_handler) {
    newchildentry_t newchild = {nil, -1};
    inside_insert(pool, get_root(pool), rid, &newchild, cmp, insert_handler);
    return newchild.newkey_rid;
}

static size_t find_j(BNode *nodeptr, RID rid, b_tree_row_row_cmp_t cmp){
    size_t j = 0;
    while(j < nodeptr->n && cmp(nodeptr->row_ptr[j], rid))
         ++j;
    return j;
}

static void del_leaf(BufferPool *pool, off_t parent_node_off, off_t node_off, BNode *node, RID rid, RID *oldchildentry, b_tree_row_row_cmp_t cmp, b_tree_insert_nonleaf_handler_t insert_handler, b_tree_delete_nonleaf_handler_t delete_handler)
{
    if (node->n > DEGREE)
    {
        size_t i = find_j(node, rid, cmp);
        memmove(node->row_ptr + i, node->row_ptr + i + 1, (node->n - i - 1) * sizeof(RID));
        --node->n;

        get_rid_block_addr(*oldchildentry) = -1;
        return;
    }

    size_t k = find_j(node, rid, cmp);
    memmove(node->row_ptr + k, node->row_ptr + k + 1, (node->n - k - 1) * sizeof(RID));
    --node->n;
    // get_rid_block_addr(*oldchildentry) = -1;
    BCtrlBlock *ctrlblock = (BCtrlBlock *)get_page(pool, 0);
    if (node_off == ctrlblock->root_node) // N is root
        return release(pool, 0);

    BNode *parent_node = (BNode *)get_page(pool, parent_node_off);
    int i = 0;
    while (i <= parent_node->n && parent_node->child[i] != node_off)
            ++i;
    off_t sibling_off = (i == 0) ? parent_node->child[1] : parent_node->child[i - 1];
    BNode *sibling_node = (BNode *)get_page(pool, sibling_off);

    if (sibling_node->n > DEGREE)
    { // redistribution
        if (i == 0)
        { // sibling at right hand
            ++node->n;
            node->row_ptr[node->n - 1] = sibling_node->row_ptr[0];
            delete_handler(parent_node->row_ptr[0]);
            parent_node->row_ptr[0] = insert_handler(sibling_node->row_ptr[1]);
            --sibling_node->n;
            memmove(sibling_node->row_ptr, sibling_node->row_ptr + 1, (sibling_node->n) * sizeof(RID));
        }
        else
        { // sibling at left hand
            memmove(node->row_ptr + 1, node->row_ptr, (node->n) * sizeof(RID));
            ++node->n;
            node->row_ptr[0] = sibling_node->row_ptr[sibling_node->n - 1];
            delete_handler(parent_node->row_ptr[i - 1]);
            parent_node->row_ptr[i - 1] = insert_handler(sibling_node->row_ptr[sibling_node->n - 1]);
            --sibling_node->n;
        }
        get_rid_block_addr(*oldchildentry) = -1;
    }
    else
    { // merge N and sibling_node
        if (i == 0)
        { // sibling at right hand
            *oldchildentry = parent_node->row_ptr[0];
            // node->row_ptr[node->n] = parent_node->row_ptr[0];
            memcpy(node->row_ptr + node->n, sibling_node->row_ptr, (sibling_node->n) * sizeof(RID));
            node->n += sibling_node->n;
            node->next = sibling_node->next;
            sibling_node->next = ctrlblock->free_node_head;
            ctrlblock->free_node_head = sibling_off;
        }
        else
        { // sibling at left hand
            *oldchildentry = parent_node->row_ptr[i - 1];
            // sibling_node->row_ptr[sibling_node->n] = parent_node->row_ptr[i-1];

            memcpy(sibling_node->row_ptr + sibling_node->n, node->row_ptr, (node->n) * sizeof(RID));
            // memcpy(sibling_node->child + sibling_node->n + 1, node->child, (node->n+1)*sizeof(off_t));
            sibling_node->n += node->n;
            sibling_node->next = node->next;
            node->next = ctrlblock->free_node_head;
            ctrlblock->free_node_head = node_off;
        }
    }
    release(pool, sibling_off);
    release(pool, parent_node_off);

    release(pool, 0); // release ctrlblock
}

static void del_nonleaf(BufferPool *pool, off_t parent_node_off, off_t node_off, RID rid, RID *oldchildentry, b_tree_row_row_cmp_t cmp, b_tree_insert_nonleaf_handler_t insert_handler, b_tree_delete_nonleaf_handler_t delete_handler)
{
    BNode *nodeptr = (BNode *)get_page(pool, node_off); // N
    size_t i = find_j(nodeptr, *oldchildentry, cmp);
    delete_handler(nodeptr->row_ptr[i]);
    memmove(nodeptr->row_ptr + i, nodeptr->row_ptr + i + 1, (nodeptr->n - i - 1) * sizeof(RID));
    memmove(nodeptr->child + i + 1, nodeptr->child + i + 2, (nodeptr->n - i - 1) * sizeof(off_t));
    --nodeptr->n;
    if (nodeptr->n >= DEGREE)
    {
        get_rid_block_addr(*oldchildentry) = -1;
        return release(pool, node_off); // release N
    }

    BCtrlBlock *ctrlblock = (BCtrlBlock *)get_page(pool, 0);
    if (node_off == ctrlblock->root_node)
    { // N is root
        if (nodeptr->n == 0)
        {
            ctrlblock->root_node = nodeptr->child[0];
            nodeptr->next = ctrlblock->free_node_head;
            ctrlblock->free_node_head = node_off;
        }
    }
    else
    {
        BNode *parent_node = (BNode *)get_page(pool, parent_node_off);
        int i = 0;
        while (i <= parent_node->n && parent_node->child[i] != node_off)
            ++i;
        off_t sibling_off = (i == 0) ? parent_node->child[1] : parent_node->child[i - 1];
        BNode *sibling_node = (BNode *)get_page(pool, sibling_off);
        if (sibling_node->n > DEGREE)
        { // redistribution
            if (i == 0)
            { // sibling at right hand
                ++nodeptr->n;
                nodeptr->row_ptr[nodeptr->n - 1] = parent_node->row_ptr[0];
                parent_node->row_ptr[0] = sibling_node->row_ptr[0];
                nodeptr->child[nodeptr->n] = sibling_node->child[0];
                --sibling_node->n;
                memmove(sibling_node->row_ptr, sibling_node->row_ptr + 1, (sibling_node->n) * sizeof(RID));
                memmove(sibling_node->child, sibling_node->child + 1, (sibling_node->n + 1) * sizeof(off_t));
            }
            else
            { // sibling at left hand
                memmove(nodeptr->row_ptr + 1, nodeptr->row_ptr, nodeptr->n * sizeof(RID));
                memmove(nodeptr->child + 1, nodeptr->child, (nodeptr->n + 1) * sizeof(off_t));
                ++nodeptr->n;
                nodeptr->row_ptr[0] = parent_node->row_ptr[i - 1];
                parent_node->row_ptr[i - 1] = sibling_node->row_ptr[sibling_node->n - 1];
                nodeptr->child[0] = sibling_node->child[sibling_node->n];
                --sibling_node->n;
            }
            get_rid_block_addr(*oldchildentry) = -1;
        }
        else
        { // merge N and sibling_node
            if (i == 0)
            { // sibling at right hand
                *oldchildentry = parent_node->row_ptr[0];
                nodeptr->row_ptr[nodeptr->n] = insert_handler(parent_node->row_ptr[0]);
                memcpy(nodeptr->row_ptr + nodeptr->n + 1, sibling_node->row_ptr, (sibling_node->n) * sizeof(RID));
                memcpy(nodeptr->child + nodeptr->n + 1, sibling_node->child, (sibling_node->n + 1) * sizeof(off_t));
                nodeptr->n += (1 + sibling_node->n);
                sibling_node->next = ctrlblock->free_node_head;
                ctrlblock->free_node_head = sibling_off;
            }
            else
            { // sibling at left hand
                *oldchildentry = parent_node->row_ptr[i - 1];
                sibling_node->row_ptr[sibling_node->n] = insert_handler(parent_node->row_ptr[i - 1]);
                memcpy(sibling_node->row_ptr + sibling_node->n + 1, nodeptr->row_ptr, (nodeptr->n) * sizeof(RID));
                memcpy(sibling_node->child + sibling_node->n + 1, nodeptr->child, (nodeptr->n + 1) * sizeof(off_t));
                sibling_node->n += (1 + nodeptr->n);
                nodeptr->next = ctrlblock->free_node_head;
                ctrlblock->free_node_head = node_off;
            }
        }
        release(pool, sibling_off);
        release(pool, parent_node_off);
    }
    release(pool, 0); // release ctrlblock

    release(pool, node_off); // release N
}

void inside_delete(BufferPool *pool, off_t parent_node_off, off_t node_off, RID rid, RID *oldchildentry, b_tree_row_row_cmp_t cmp, b_tree_insert_nonleaf_handler_t insert_handler, b_tree_delete_nonleaf_handler_t delete_handler){
    BNode *node = (BNode*)get_page(pool,node_off);
    if(node->leaf == '1'){ //leaf node
        del_leaf(pool,parent_node_off,node_off,node,rid,oldchildentry,cmp,insert_handler,delete_handler);
        release(pool,node_off);
        return;
    }
    //non-leaf node
    off_t child_off;
    if(cmp(rid, node->row_ptr[0]) < 0){
        child_off = node->child[0];
    }
    else if(cmp(rid, node->row_ptr[node->n-1]) >= 0){
        child_off = node->child[node->n];
    }
    else{
        for(int i=0;i<node->n-1;++i)
            if(cmp(rid, node->row_ptr[i]) >= 0 && cmp(rid, node->row_ptr[i+1]) < 0){
                child_off = node->child[i+1];
                break;
            }
    }
    release(pool,node_off);        
    inside_delete(pool,node_off,child_off,rid,oldchildentry,cmp,insert_handler,delete_handler);

    if(get_rid_block_addr(*oldchildentry) == -1)
        return;
    del_nonleaf(pool,parent_node_off,node_off,rid,oldchildentry,cmp,insert_handler,delete_handler);
}

void b_tree_delete(BufferPool *pool, RID rid, b_tree_row_row_cmp_t cmp, b_tree_insert_nonleaf_handler_t insert_handler, b_tree_delete_nonleaf_handler_t delete_handler) {
    RID oldchildentry = nil;
    inside_delete(pool,-1,get_root(pool),rid,&oldchildentry,cmp,insert_handler,delete_handler);
}
