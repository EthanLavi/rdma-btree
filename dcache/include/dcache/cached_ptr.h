#pragma once

// #include "object_pool.h"
#include <atomic>
#include <remus/logging/logging.h>
#include <remus/rdma/rdma.h>
#include <remus/rdma/rdma_ptr.h>

using namespace remus::rdma;
using namespace std;

typedef std::atomic<int> ref_t;

/// Cached object is given responsibility for decrementing the reference when it goes out of scope
/// It can also only be moved so it will only ever decrease the value
template <typename T>
class CachedObject {
private:
    ref_t* ref_count;
    rdma_ptr<T> obj;
    bool temp; // true if needs manual deallocation and isn't stored in a cache line
    std::function<void()> dealloc;
public:
    // Constructors
    CachedObject() {
        ref_count = nullptr;
        obj = nullptr;
        temp = false;
    };

    CachedObject(rdma_ptr<T> obj, ref_t* ref_count) : obj(obj), temp(false), ref_count(ref_count) {}
    CachedObject(rdma_ptr<T> obj, std::function<void()> deallocator) : obj(obj), temp(true), ref_count(nullptr), dealloc(deallocator) {}

    // delete copy but allow move
    CachedObject(CachedObject& o) = delete;
    CachedObject &operator=(CachedObject& o) = delete;

    CachedObject(CachedObject&& o) {
        // Invalidate the moved from object since it takes ownership
        this->ref_count = o.ref_count;
        this->obj = o.obj;
        this->temp = o.temp;
        this->dealloc = o.dealloc;
        o.ref_count = nullptr;
        o.obj = nullptr;
        o.temp = false;
    }

    CachedObject &operator=(CachedObject&& o){
        if (temp){
            dealloc();
        }
        if (ref_count != nullptr) {
            int refs = ref_count->fetch_sub(1) - 1;
            REMUS_ASSERT_DEBUG(refs >= 0, "Reference count became negative");
        }
        // Swap object fields
        this->ref_count = o.ref_count;
        this->obj = o.obj;
        this->temp = o.temp;
        this->dealloc = o.dealloc;
        o.ref_count = nullptr;
        o.obj = nullptr;
        o.temp = false;
        return *this;
    }

    // Pointer-like functions
    static constexpr T *to_address(const CachedObject& p) { return (T*) p.obj.address(); }
    static constexpr CachedObject pointer_to(T& p) { return CachedObject(-1, &p); }
    T* get() const { return (T*) obj.address(); }
    T* operator->() const noexcept { return (T*) obj.address(); }
    T& operator*() const noexcept { return *((T*) obj.address()); }

    // Compare the two
    constexpr bool operator==(CachedObject<T> o) const volatile { return o.obj == obj; }

    template <typename U> friend std::ostream &operator<<(std::ostream &os, const CachedObject<U> &p);

    ~CachedObject(){
        if (temp){
            dealloc();
            return;
        }
        if (ref_count == nullptr) return; // guard against empty objects
        // Reduce number of references and deallocate if necessary
        int refs = ref_count->fetch_sub(1) - 1;
        REMUS_ASSERT(refs >= 0, "Reference count became negative");
    }

    /// The pointer returned by this object lives as long as the object is alive
    /// If it returns from the cache, we might let the pointer live longer as it is readonly
    rdma_ptr<T> get(){
        return obj;
    }

    int get_ref_count(){
        if (ref_count == nullptr) return 0;
        return *ref_count;
    }
};

/// Operator support for printing a rdma_ptr<U>
template <typename U> std::ostream &operator<<(std::ostream &os, const CachedObject<U> &p) {
  return os << p.obj;
}
