#include <cassert>
#include <iostream>

#include <rome/rdma/rdma.h>
#include <rome/rdma/rdma_ptr.h>
#include "../include/mark_ptr.h"

using namespace rome::rdma; 
using namespace std;

/// tmp object
struct alignas(64) Object {
    long data[8];
};

int main(){
    rdma_ptr<Object> o = nullptr;
    assert(!is_marked(o));
    mark_ptr(o);
    assert(is_marked(o));
    unmark_ptr(o);
    assert(!is_marked(o));

    cout << "Passed all asserts!" << endl;
}