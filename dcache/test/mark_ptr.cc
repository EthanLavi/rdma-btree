#include <remus/rdma/rdma.h>
#include <remus/rdma/rdma_ptr.h>

#include <dcache/mark_ptr.h>

using namespace remus::rdma; 

/// tmp object
struct alignas(64) Object {
    long data[8];
};

int main(){
    rdma_ptr<Object> o = rdma_ptr<Object>(0x0eadbeefdeadbeef);
    
    REMUS_INFO("Ptr1 = {}", o);
    REMUS_ASSERT(!is_marked(o), "Pointer started out unmarked but is treated as marked");
    
    o = mark_ptr(o);
    REMUS_INFO("Ptr2 = {}", o);
    REMUS_ASSERT(is_marked(o), "Pointer isn't marked when it should be");

    o = unmark_ptr(o);
    REMUS_INFO("Ptr3 = {}", o);
    REMUS_ASSERT(!is_marked(o), "Marked pointer cannot become unmarked");
    REMUS_ASSERT(o.raw() == 0x0eadbeefdeadbeef, "Some other bits were affected");

    // Plus operator increases by sizeof(Object)
    rdma_ptr<Object> j = rdma_ptr<Object>(0, nullptr);
    j += 1;
    REMUS_ASSERT(j.raw() == 64, "Raw is incorrect");
    j += 10;
    REMUS_ASSERT(j.raw() == 11 * 64, "Raw is incorrect");

    // Plus operator again increases by size of object to 21 Objects past null
    rdma_ptr<Object> a1(0, 21 * 64);
    rdma_ptr<Object> a2 = j + 10;
    REMUS_ASSERT(a1 == a2, "Different pointers is unexpected result");

    // Can use [] index to achieve same result as before (11th object)
    j = rdma_ptr<Object>(2, nullptr);
    REMUS_ASSERT(j[11].address() == (64 * 11), "Incorrect address");
    REMUS_ASSERT(j[11].id() == 2, "Incorrect id");

    REMUS_INFO("Test 1 -- PASSED");

    return 0;
}