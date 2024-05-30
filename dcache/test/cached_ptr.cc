#include <remus/logging/logging.h>
#include <remus/rdma/rdma.h>

#include <dcache/cached_ptr.h>
#include "faux_mempool.h"

using namespace remus::rdma;
using namespace std;

struct alignas(64) BigInt {
    long lower;
    long upper;
};

struct Object {};

typedef CachedObject<BigInt> CachedPtr;

void test1(){
    // Run operations
    CachedPtr obj = CachedPtr();
    REMUS_ASSERT(obj.get_ref_count() == 0, "Ref count is not 0 for a default object");
}

void test2(CountingPool* pool){
    rdma_ptr<BigInt> ptrA = pool->Allocate<BigInt>();
    rdma_ptr<BigInt> ptrB = pool->Allocate<BigInt>();
    rdma_ptr<BigInt> ptrC = pool->Allocate<BigInt>();
    std::atomic<int> ref_count;
    ref_count.store(3);
    {
        CachedPtr objA = CachedPtr(ptrA, 1, &ref_count);
        CachedPtr objB = CachedPtr(ptrA, 1, &ref_count);
        CachedPtr objC = CachedPtr(ptrA, 1, &ref_count);
    }
    REMUS_ASSERT(ref_count.load() == 0, "Ref count is not 0, invalid number of decrements");
    pool->Deallocate(ptrA);
    pool->Deallocate(ptrB);
    pool->Deallocate(ptrC);
}

CachedPtr copy_over(CachedPtr org){
    REMUS_ASSERT(org.get_ref_count() == 1, "Exists just here, but will be returned");
    return org;
}

void test3(CountingPool* pool){
    rdma_ptr<BigInt> ptr = pool->Allocate<BigInt>();
    std::atomic<int> ref_count;
    ref_count.store(1);
    {
    CachedPtr obj2 = CachedPtr(ptr, 1, &ref_count);
    CachedPtr obj3 = std::move(obj2);
    REMUS_ASSERT(obj3.get_ref_count() == 1, "Ref count is not the same (1) after move");
    REMUS_ASSERT(ref_count.load() == 1, "Number of decrements was invalid");
    CachedPtr obj4 = copy_over(std::move(obj3));
    REMUS_ASSERT(obj4.get_ref_count() == 1, "Ref count is not 1, return from function caused increment/decrement");
    REMUS_ASSERT(ref_count.load() == 1, "Number of decrements was invalid");
    }
    REMUS_ASSERT(ref_count.load() == 0, "Number of decrements was invalid");
    pool->Deallocate(ptr);
}

void test4(CountingPool* pool){
    rdma_ptr<BigInt> ptr2 = pool->Allocate<BigInt>();
    std::atomic<int> ref_count;
    ref_count.store(1);
    {
    CachedPtr obj5 = CachedPtr(ptr2, 1, &ref_count);
    {
    REMUS_ASSERT(obj5.get_ref_count() == 1, "Ref count is not 1 after construction");
    CachedPtr obj5_move = std::move(obj5);
    REMUS_ASSERT(obj5_move.get_ref_count() == 1, "Ref count is not the same after move (1)");
    REMUS_ASSERT(ref_count.load() == 1, "Ref count is not the same after move (2)");
    REMUS_ASSERT(obj5.get() == nullptr, "Move didn't leave the original object in a valid but null state");
    }
    REMUS_ASSERT(ref_count.load() == 0, "Object was not destructed properly");
    }
    REMUS_ASSERT(ref_count.load() == 0, "Object was not destructed properly");
    pool->Deallocate(ptr2);
}

void test5(CountingPool* pool){
    rdma_ptr<BigInt> ptr3 = pool->Allocate<BigInt>();
    rdma_ptr<BigInt> ptr4 = pool->Allocate<BigInt>();

    std::atomic<int> ref_count1;
    ref_count1.store(1);
    std::atomic<int> ref_count2;
    ref_count2.store(1);

    {
    CachedPtr obj6 = CachedPtr(ptr3, 1, &ref_count1);
    CachedPtr obj7 = CachedPtr(ptr4, 1, &ref_count2);
    obj6 = std::move(obj7);
    }

    REMUS_ASSERT(ref_count1.load() == 0, "Number of decrements was invalid");
    REMUS_ASSERT(ref_count2.load() == 0, "Number of decrements was invalid");
    pool->Deallocate(ptr3);
    pool->Deallocate(ptr4);
}

void test6(CountingPool* pool){
    rdma_ptr<BigInt> ptr5 = pool->Allocate<BigInt>(3);
    std::atomic<int> ref_count;
    ref_count.store(1);
    {
    CachedPtr obj8 = CachedPtr(ptr5, 3, &ref_count);
    }
    REMUS_ASSERT(ref_count.load() == 0, "Number of decrements was invalid");
    pool->Deallocate(ptr5, 3);
}

void test7(CountingPool* pool){
    rdma_ptr<BigInt> ptr6 = pool->Allocate<BigInt>();
    std::atomic<int> ref_count;
    ref_count.store(1);
    {
    CachedObject<BigInt> obj9 = CachedObject<BigInt>(ptr6, 1, &ref_count);
    CachedObject<BigInt> obj10;
    obj10 = std::move(obj9);
    }
    REMUS_ASSERT(ref_count.load() == 0, "Number of decrements was invalid");
    pool->Deallocate(ptr6);
}

void scope(CountingPool* pool){
    test1();
    REMUS_INFO("Test 1 -- PASSED");
    test2(pool);
    REMUS_INFO("Test 2 -- PASSED");
    test3(pool);
    REMUS_INFO("Test 3 -- PASSED");
    test4(pool);
    REMUS_INFO("Test 4 -- PASSED");
    test5(pool);
    REMUS_INFO("Test 5 -- PASSED");
    test6(pool);
    REMUS_INFO("Test 6 -- PASSED");
    test7(pool);
    REMUS_INFO("Test 7 -- PASSED");
}

int main(){
    CountingPool* pool = new CountingPool(true);
    REMUS_INFO("Constructed pool");

    rdma_ptr<BigInt> start = pool->Allocate<BigInt>();
    REMUS_ASSERT(start != nullptr, "Cannot allocate");
    scope(pool);
    // todo: check to_address, * and -> operators?
    // Test our first allocation
    pool->Deallocate(start);
    if (pool->HasNoLeaks()){
        REMUS_INFO("No Leaks In Cached Pointer");
    } else {
        REMUS_ERROR("Found Leaks in Cached Pointer");
        pool->debug();
        return 1;
    }
    return 0;
}