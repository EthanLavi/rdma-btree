#pragma once

#include <remus/rdma/rdma.h>
#include <remus/rdma/rdma_ptr.h>

using namespace remus::rdma;
using namespace std;

/// It's not a problem to share via pointer. Just can only be deconstructed once
/// A wrapper around rdma_ptr to handle conditional deallocation
template <typename T>
class CachedObject {
private:
    rdma_capability* pool;  // todo: make static so that we don't pass around pool?
    bool temporary;
    rdma_ptr<T> obj;
    int size;

public:
    CachedObject() = default;
    CachedObject(rdma_capability* pool, rdma_ptr<T> obj, int size, bool temp) : pool(pool), obj(obj), size(size), temporary(temp) {}

    // delete copy but allow move
    CachedObject(CachedObject& o) = delete;
    CachedObject &operator=(CachedObject&) = delete;
    CachedObject(CachedObject&& o) {
        if (this->temporary){
            pool->Deallocate(obj, size);
        }
        this->size = o.size;
        this->obj = o.obj;
        this->temporary = o.temporary;
        this->pool = o.pool;
        o.temporary = false;
    }
    CachedObject &operator=(CachedObject&& o){
        if (this->temporary){
            pool->Deallocate(obj, size);
        }
        this->size = o.size;
        this->obj = o.obj;
        this->temporary = o.temporary;
        this->pool = o.pool;
        o.temporary = false;
        return *this;
    }

    // Pointer-like functions
    static constexpr T *to_address(const CachedObject& p) { return (T*) p.obj.address(); }
    static constexpr CachedObject pointer_to(T& p) { return CachedObject(-1, &p); }
    T* get() const { return (T*) obj.address(); }
    T* operator->() const noexcept { return (T*) obj.address(); }
    T& operator*() const noexcept { return *((T*) obj.address()); }

    template <typename U> friend std::ostream &operator<<(std::ostream &os, const CachedObject<U> &p) {
        return os << p.obj;
    }

    ~CachedObject(){
        if (temporary){
            pool->Deallocate(obj, size);
        }
    }

    /// The pointer returned by this object lives as long as the object is alive
    /// If it returns from the cache, we might let the pointer live longer as it is readonly
    rdma_ptr<T> get(){
        return obj;
    }
};