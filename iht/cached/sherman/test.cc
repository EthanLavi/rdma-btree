#include "lockfree_sk.h"
#include <iostream>

int main(){
    LockFreeSkiplist ll = LockFreeSkiplist(1);
    for(long i = 0; i < 1000; i++){
        ll.insert(i, i + 10, (void*) i);
    }
    std::cout << ll.get(15, nullptr) << std::endl;
    Node* it = ll.begin();
    while(it != ll.end()){
        std::cout << it->key_low << " " << it->key_high << std::endl;
        it = ll.next(it);
    }

}