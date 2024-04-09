#include <cassert>
#include <iostream>

#include <remus/rdma/rdma.h>
#include <remus/rdma/rdma_ptr.h>
#include "../include/mark_ptr.h"

using namespace remus::rdma; 
using namespace std;

/// tmp object
struct alignas(64) Object {
    long data[8];
};

int main(){
    rdma_ptr<Object> o = rdma_ptr<Object>(0x0eadbeefdeadbeef);
    
    cout << o << endl;
    assert(!is_marked(o));
    
    o = mark_ptr(o);
    cout << o << endl;
    assert(is_marked(o));

    o = unmark_ptr(o);
    cout << o << endl;
    assert(!is_marked(o));
    assert(o.raw() == 0x0eadbeefdeadbeef);
    cout << o << endl;

    rdma_ptr<Object> j = rdma_ptr<Object>(0, nullptr);
    j += 1;
    assert(j.raw() == 64);
    j += 10;
    assert(j.raw() == 11 * 64);
    rdma_ptr<Object> a1(0, 21 * 64);
    rdma_ptr<Object> a2 = j + 10;
    assert(a1 == a2);
    j = rdma_ptr<Object>(2, nullptr);
    assert(j[11].address() == (64 * 11));
    assert(j[11].id() == 2);

    cout << "Passed all asserts!" << endl;
}