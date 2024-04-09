#include <remus/rdma/rdma.h>
#include <remus/rdma/rdma_ptr.h>

using namespace remus::rdma;
using namespace std;

/// It's not a problem to share via pointer. Just can only be deconstructed once
/// A wrapper around rdma_ptr to handle conditional deallocation
template <typename T>
class CachedObject {
private:
    shared_ptr<rdma_capability> pool; // todo: make static so that we don't pass around pool
    rdma_ptr<T> obj;
    bool temporary;

public:
    CachedObject(shared_ptr<rdma_capability> pool, rdma_ptr<T> obj, bool temp) : pool(pool), obj(obj), temporary(temp) {}
    
    // delete copy and move to prevent use after free
    CachedObject(CachedObject& o) = delete;
    CachedObject(CachedObject&& o) = delete;
    CachedObject &operator=(const CachedObject&) = delete;
    CachedObject &operator=(const CachedObject&&) = delete; // ? is this valid

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
            pool->Deallocate(obj);
        }
    }

    /// The pointer returned by this object lives as long as the object is alive
    /// If it returns from the cache, we might let the pointer live longer as it is readonly
    rdma_ptr<T> get(){
        return obj;
    }
};