#include <remus/rdma/rdma.h>

#include "../../common.h"

using namespace remus::rdma;

template <class K>
class ShermanRoot {
private:
    struct RootEntry {
        bool present;
        rdma_ptr<anon_ptr> ptr;
        K key_low;
        K key_high;
    };
    int size;
    RootEntry* ptrs;

public:
    ShermanRoot(int size) : size(size) {
        ptrs = new RootEntry[size];
        for(int i = 0; i < size; i++){
            ptrs[i].present = false;
        }
    }

    rdma_ptr<anon_ptr> find_ptr(K key){
        for(int i = 0; i < size; i++){
            if (!ptrs[i].present) continue;
            if (ptrs[i].key_low <= key && key < ptrs[i].key_high) { // if key is in range
                return ptrs[i].ptr;
            }
        }
        return nullptr;
    }

    bool put_into_cache(K key_low, K key_high, rdma_ptr<anon_ptr> ptr){
        for(int i = 0; i < size; i++){
            if (ptrs[i].present) continue;
            ptrs[i].present = true;
            ptrs[i].key_low = key_low;
            ptrs[i].key_high = key_high;
            ptrs[i].ptr = ptr;
            return true;
        }
        return false;
    }

    void invalidate(K key){
        for(int i = 0; i < size; i++){
            if (!ptrs[i].present) continue;
            if (ptrs[i].key_low <= key && key < ptrs[i].key_high) { // if key is in range
                ptrs[i].present = false;
            }
        }
    }
};