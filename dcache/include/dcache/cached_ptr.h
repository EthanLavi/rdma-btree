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
    template<typename U>
    friend class CachedObject;
private:
    ref_t* ref_count;
    rdma_ptr<T> obj;
    int size;
public:
    // Constructors
    CachedObject() {
        ref_count = nullptr;
        obj = nullptr;
    };

    CachedObject(rdma_ptr<T> obj, int size, ref_t* ref_count) : obj(obj), size(size), ref_count(ref_count) {}

    // delete copy but allow move
    CachedObject(CachedObject& o) = delete;
    CachedObject &operator=(CachedObject& o) = delete;

    CachedObject(CachedObject&& o) {
        // Invalidate the moved from object since it takes ownership
        this->size = o.size;
        this->ref_count = o.ref_count;
        this->obj = o.obj;
        o.size = 0;
        o.ref_count = nullptr;
        o.obj = nullptr;
    }

    CachedObject &operator=(CachedObject&& o){
        if (ref_count != nullptr) {
            int refs = ref_count->fetch_sub(1) - 1;
            REMUS_ASSERT_DEBUG(refs >= 0, "Reference count became negative");
        }
        // Swap object fields
        this->size = o.size;
        this->ref_count = o.ref_count;
        this->obj = o.obj;
        o.size = 0;
        o.ref_count = nullptr;
        o.obj = nullptr;
        return *this;
    }

    template <typename U>
    CachedObject<T>& operator=(CachedObject<U>&& o) {
        // Swap object fields
        std::swap(this->size, o.size);
        std::swap(this->ref_count, o.ref_count);
        uint64_t tmp = this->obj.raw();
        this->obj = rdma_ptr<T>(o.obj.raw());
        o.obj = rdma_ptr<U>(tmp);
        return *this;
    }

    // Pointer-like functions
    static constexpr T *to_address(const CachedObject& p) { return (T*) p.obj.address(); }
    static constexpr CachedObject pointer_to(T& p) { return CachedObject(-1, &p); }
    T* get() const { return (T*) obj.address(); }
    T* operator->() const noexcept { return (T*) obj.address(); }
    T& operator*() const noexcept { return *((T*) obj.address()); }

    // Compare the two
    constexpr bool operator==(CachedObject<T> o) const volatile { return o.obj == obj && o.size == size; }

    template <typename U> friend std::ostream &operator<<(std::ostream &os, const CachedObject<U> &p);

    ~CachedObject(){
        if (ref_count == nullptr) return; // guard against empty objects
        // Reduce number of references and deallocate if necessary
        int refs = ref_count->fetch_sub(1) - 1;
        REMUS_ASSERT_DEBUG(refs >= 0, "Reference count became negative");
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
