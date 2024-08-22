#include <climits>
#include <cstdint>
#include <atomic>
#include <queue>
#include <iostream>

template <class T>
struct LocalLimboLists {
    std::atomic<std::queue<T*>*> free_lists[3];
    LocalLimboLists() = default;
};

/// An object pool that can reallocate objects as well
template <class T, int OPS_PER_EPOCH>
class LocalEBR {
    /// version that the node can wait on. Once it sees it increments, in can rotate its free lists and increment the next node
    struct alignas(64) ebr_ref {
        long version;
    };

    int thread_count;
    std::atomic<int> id_gen; // generator for the id
    ebr_ref* thread_slots; // each thread has a local version
    std::atomic<int> at_version;
    static thread_local int id; // the id matches a local version in the version_slots
    static thread_local int counter;
    static thread_local LocalLimboLists<T>* limbo; // thread queues to use as free-lists

public:
    LocalEBR(int thread_count) : thread_count(thread_count) {
        id_gen.store(0);
        thread_slots = new ebr_ref[thread_count]();
        for(int i = 0; i < thread_count; i++){
            thread_slots[i].version = 0;
        }
        at_version.store(0);
    }

    ~LocalEBR(){
        delete[] thread_slots;
    }

    // Get an id for the thread...
    void RegisterThread(){
        id = id_gen.fetch_add(1);
        counter = 0;
        if (id >= thread_count) {
            std::cout << "Too many registered threads for EBR" << std::endl;
            abort();
        }
        LocalEBR<T, OPS_PER_EPOCH>::limbo = new LocalLimboLists<T>();
        limbo->free_lists[0] = new std::queue<T*>();
        limbo->free_lists[1] = new std::queue<T*>();
        limbo->free_lists[2] = new std::queue<T*>();
    }

    /// Called at the end of every operation to indicate a finish 
    void match_version(bool override_epoch = false){
        if (id == -1) {
            RegisterThread();
        }
        counter++;
        if (override_epoch || counter % OPS_PER_EPOCH != 0) return; // wait before cycling iterations
        long new_version = at_version.load() + 1;
        // If my version is behind the current version, hop ahead
        if (thread_slots[id].version < new_version){
            thread_slots[id].version = new_version;

            // cycle free lists (its fine if deallocations gets pushed back two iterations)
            auto tmp = limbo->free_lists[0].load();
            limbo->free_lists[0].store(limbo->free_lists[1].load());
            limbo->free_lists[1].store(limbo->free_lists[2].load());
            limbo->free_lists[2].store(tmp);
        }

        // Guard agaisnt a behind-thread
        for(int i = 0; i < thread_count; i++)
            if (thread_slots[i].version != new_version) return;

        // All threads are up-to-date.
        // If the next-node version is not update_to_date, try to be the one to write to it
        if (new_version > at_version.load()){
            int v = at_version.exchange(new_version);
        }
    }

    /// Requeue something that was popped but wasn't used
    void requeue(T* obj){
        if (id == -1) {
            RegisterThread();
        }
        limbo->free_lists[0].load()->push(obj);
    }

    /// Add to the end of the free lists (unsynchronized)
    void deallocate(T* obj){
        if (id == -1) {
            RegisterThread();
        }
        limbo->free_lists[2].load()->push(obj); // add to the free list
    }

    /// Guaranteed via EBR to be exclusive
    T* allocate(){
        if (id == -1) {
            RegisterThread();
        }
        if (limbo->free_lists[0].load()->empty()){
            return (T*) malloc(sizeof(T));
        } else {
            T* ret = limbo->free_lists[0].load()->front();
            limbo->free_lists[0].load()->pop();
            return ret;
        }
    }
};

template <class T, int OPS_PER_EPOCH>
thread_local int LocalEBR<T, OPS_PER_EPOCH>::id = -1;

template <class T, int OPS_PER_EPOCH>
thread_local int LocalEBR<T, OPS_PER_EPOCH>::counter = 0;

template <class T, int OPS_PER_EPOCH>
thread_local LocalLimboLists<T>* LocalEBR<T, OPS_PER_EPOCH>::limbo = nullptr;

class Node {
public:
    int topLevel;
    static const int MAX_LEVEL = 16;

    void* value;
    int key_low;
    int key_high;
    std::atomic<int> accesses;
    std::atomic<Node*> next[MAX_LEVEL];

    Node(){}

    Node(int key_low, int key_high, void* value, int height) : key_low(key_low), key_high(key_high), value(value) {
        for(int i = 0; i < MAX_LEVEL; i++){
            next[i].store(nullptr);
        }
        topLevel = height;
        accesses = 0;
    }

    bool is_less_than(int key){
        return key_high <= key;
    }

    bool is_less_than(int other_key_low, int other_key_high){
        if (other_key_high <= key_low) return false;
        if (key_high <= other_key_low) return true;
        return false;
    }

    bool is_equal(int key){
        return key_low <= key && key < key_high;
    }

    bool is_equal(int other_key_low, int other_key_high){
        if (other_key_high <= key_low) return false;
        if (key_high <= other_key_low) return false;
        return true;
    }
};

class LockFreeSkiplist {
    int randomLevel() {
        static thread_local uint32_t y = 2463534242;
        y^=(y<<13);
        y^=(y>>17);
        y^=(y<<5);
        uint32_t temp = y;
        uint32_t level = 0;
        while (((temp >>= 1) & 1) != 0) {
            ++level;
        }
        /* 1 <= level < MAX_HEIGHT */
        if (level >= MAX_LEVEL) {
            return (int) MAX_LEVEL - 1;
        } else {
            return (int) level;
        }
    }

    inline Node* IsMarked(Node* ptr, bool* is_marked){
        *is_marked = (uint64_t) ptr & 0x1;
        return (Node*) ((uint64_t) ptr & ~0x1);
    }

    inline Node* Marked(Node* ptr){
        return (Node*) ((uint64_t) ptr | 0x1);
    }

    inline Node* Unmark(Node* ptr){
        return (Node*) ((uint64_t) ptr & ~0x1);
    }

    LocalEBR<Node, 1000>* local_ebr;
public:
    static const int MAX_LEVEL = 16;

    Node* head = new Node(INT_MIN, INT_MIN, nullptr, MAX_LEVEL);
    Node* tail = new Node(INT_MAX, INT_MAX, nullptr, MAX_LEVEL);

    LockFreeSkiplist(int thread_count){
        for(int i = 0; i < MAX_LEVEL; i++){
            head->next[i].store(tail);
        }
        local_ebr = new LocalEBR<Node, 1000>(thread_count);
    }

    ~LockFreeSkiplist(){
        delete local_ebr;
        delete head;
        delete tail;
    }

    bool find(int key_lb, int key_ub, Node* preds[], Node* succs[]){
        int bottomLevel = 0;
        bool marked = false;
        bool snip;
        Node* pred = nullptr;
        Node* curr = nullptr;
        Node* succ = nullptr;
        retry:
        while(true){
            pred = head;
            for(int level = MAX_LEVEL - 1; level >= bottomLevel; level--){
                curr = Unmark(pred->next[level].load());
                while(true){
                    succ = IsMarked(curr->next[level].load(), &marked);
                    while(marked){
                        snip = pred->next[level].compare_exchange_weak(curr, succ);
                        if (!snip) goto retry;
                        curr = Unmark(pred->next[level].load());
                        succ = IsMarked(curr->next[level].load(), &marked);
                    }
                    if (curr->is_less_than(key_lb, key_ub)){
                        pred = curr;
                        curr = succ;
                    } else {
                        break;
                    }
                }
                preds[level] = pred;
                succs[level] = curr;
            }
            return curr->is_equal(key_lb, key_ub);
        }
    }

    bool insert(int key_lb, int key_ub, void* x){
        int topLevel = randomLevel();
        int bottomLevel = 0;
        Node* preds[MAX_LEVEL];
        Node* succs[MAX_LEVEL];
        Node* newNode = local_ebr->allocate();
        newNode->key_low = key_lb;
        newNode->key_high = key_ub;
        newNode->value = x;
        newNode->accesses = 0;
        for(int i = 0; i < MAX_LEVEL; i++){
            newNode->next[i].store(nullptr);
        }
        newNode->topLevel = topLevel;
        while(true){
            bool found = find(key_lb, key_ub, preds, succs);
            if (found){
                local_ebr->match_version();
                local_ebr->requeue(newNode);
                return false;
            } else {
                for(int level = bottomLevel; level <= topLevel; level++){
                    Node* succ = succs[level];
                    newNode->next[level].store(Unmark(succ));
                }
                Node* pred = preds[bottomLevel];
                Node* succ = succs[bottomLevel];
                if (!pred->next[bottomLevel].compare_exchange_weak(succ, newNode)){
                    continue;
                }
                for(int level = bottomLevel + 1; level <= topLevel; level++){
                    while(true){
                        pred = preds[level];
                        succ = succs[level];
                        if (pred->next[level].compare_exchange_weak(succ, newNode)) break;
                        find(key_lb, key_ub, preds, succs);
                    }
                }
                local_ebr->match_version();
                return true;
            }
        }
    }

    /// Returns old data, if a match and remove
    void* remove(int key_lb, int key_ub){
        int bottomLevel = 0;
        Node* preds[MAX_LEVEL];
        Node* succs[MAX_LEVEL];
        Node* succ;
        while(true){
            bool found = find(key_lb, key_ub, preds, succs);
            if (!found){
                local_ebr->match_version();
                return nullptr;
            } else {
                Node* nodeToRemove = succs[bottomLevel];
                for (int level = nodeToRemove->topLevel; level >= bottomLevel + 1; level--){
                    bool marked = false;
                    succ = IsMarked(nodeToRemove->next[level].load(), &marked);
                    while(!marked){
                        nodeToRemove->next[level].compare_exchange_weak(succ, Marked(succ));
                        succ = IsMarked(nodeToRemove->next[level].load(), &marked);
                    }
                }
                bool marked = false;
                succ = IsMarked(nodeToRemove->next[bottomLevel].load(), &marked);
                while(true){
                    bool iMarkedIt = nodeToRemove->next[bottomLevel].compare_exchange_weak(succ, Marked(succ));
                    succ = IsMarked(succs[bottomLevel]->next[bottomLevel].load(), &marked);
                    if (iMarkedIt){
                        find(key_lb, key_ub, preds, succs);
                        local_ebr->deallocate(nodeToRemove);
                        local_ebr->match_version();
                        return nodeToRemove->value;
                    } else if (marked) {
                        local_ebr->match_version();
                        return nullptr;
                    }
                }
            }
        }
    }

    void* get(int key, int* accesses){
        int bottomLevel = 0;
        bool marked = false;
        Node* pred = head;
        Node* curr = nullptr;
        Node* succ = nullptr;
        for(int level = MAX_LEVEL - 1; level >= bottomLevel; level--){
            curr = Unmark(pred->next[level].load());
            while(true){
                succ = IsMarked(curr->next[level].load(), &marked);
                while(marked){
                    curr = Unmark(pred->next[level].load());
                    succ = IsMarked(curr->next[level].load(), &marked);
                }
                if (curr->is_less_than(key)){
                    pred = curr;
                    curr = succ;
                } else {
                    break;
                }
            }
        }
        if (curr->is_equal(key)){
            if (accesses != nullptr){
                *accesses = curr->accesses.fetch_add(1);
            }
        }
        void* result = curr->is_equal(key) ? curr->value : nullptr;
        local_ebr->match_version();
        return result;
    }

    Node* begin(){
        return head;
    }

    Node* next(Node* prev){
        return Unmark(prev->next[0]);
    }

    Node* end(){
        return tail;
    }

    int count(){
        int count = 0;
        Node* curr = begin();
        while(curr != end()){
            curr = next(curr);
            count++;
        }
        return count;
    }
};