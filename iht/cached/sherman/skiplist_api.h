// Cleaner API
#include "skiplist.h"
#include <cstdlib>

template <class T>
class SkipList {
private:
    skiplist_raw sk_;

    struct my_node {
        // Metadata for skiplist node.
        skiplist_node snode;
        // My data here: {int, int} pair.
        int to;
        int from;
        // Specialized data
        T val;
    };

    static int skiplist_cmp_t(skiplist_node *a, skiplist_node *b, void *aux){
        // Get `my_node` from skiplist node `a` and `b`.
        my_node* aa = _get_entry(a, my_node, snode);
        my_node* bb = _get_entry(b, my_node, snode);

        if (aa->to == aa->from){
            int pq = aa->to;
            // point query is different
            if (pq < bb->from) return -1;
            if (pq >= bb->to) return 1;
            return 0;
        } else if (bb->to == bb->from){
            int pq = bb->to;
            // point query is different
            if (pq < aa->from) return 1;
            if (pq >= aa->to) return -1;
            return 0;
        }

        // sort by range, overlapping ranges are equal to prevent overlapping entries from being inserted
        // aa < bb: return neg
        // aa == bb: return 0
        // aa > bb: return pos
        if (aa->to <= bb->from) return -1;
        if (bb->to <= aa->from) return 1;
        return 0;
    }
public:
    SkipList(){
        skiplist_init(&sk_, skiplist_cmp_t);
    }

    bool insert(int from, int to, T val){
        if (from == to) return false; // can't insert non-ranges
        my_node* node = (my_node*) malloc(sizeof(my_node));
        node->to = to;
        node->from = from;
        node->val = val;
        skiplist_init_node(&node->snode);
        bool result = skiplist_insert_nodup(&sk_, &node->snode);
        return result == 0;
    }

    bool remove(int from, int to, T* ret_val){
        // Define a query.
        my_node query;
        query.from = from;
        query.to = to;
        // Find a skiplist node `cursor`.
        skiplist_node* cursor = skiplist_find(&sk_, &query.snode);
        if (!cursor) return false;

        // Get `my_node` from `cursor`.
        // Note: found->snode == *cursor
        my_node* found = _get_entry(cursor, my_node, snode);
        *ret_val = found->val;

        // Detach `found` from skiplist.
        skiplist_erase_node(&sk_, &found->snode);
        // Release `found`, to free its memory.
        skiplist_release_node(&found->snode);
        // Free `found` after it becomes safe.
        skiplist_wait_for_free(&found->snode);
        skiplist_free_node(&found->snode);
        free(found);
        return true;
    }

    bool get(int key, T* ret_val){
        my_node query;
        query.to = key;
        query.from = key;
        // Find a skiplist node `cursor`.
        skiplist_node* cursor = skiplist_find(&sk_, &query.snode);

        // If `cursor` is NULL, key doesn't exist.
        if (!cursor) return false;

        // Get `my_node` from `cursor`.
        // Note: found->snode == *cursor
        my_node* found = _get_entry(cursor, my_node, snode);
        *ret_val = found->val;

        // Release `cursor` (== &found->snode).
        // Other thread cannot free `cursor` until `cursor` is released.
        skiplist_release_node(cursor);
        return true;
    }
};