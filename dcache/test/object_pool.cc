#include "../include/object_pool.h"

#include <functional>
#include <remus/logging/logging.h>

void test(bool condition, const char* message){
    if (!condition){
        REMUS_ERROR("Error: {}", message);
        exit(1);
    }
}

int main(){
    ObjectPool<int> pool = ObjectPool<int>(std::function<int()>([=](){
        static int gen = 1;
        return gen++;
    }));
    
    test(pool.fetch() == 1, "Empty pool calls generate");
    test(pool.fetch() == 2, "Empty pool still calls generate");
    pool.release(10);
    test(pool.fetch() == 10, "Release adds back to the pool");
    test(pool.fetch() == 3, "Pool is now empty");

    REMUS_INFO("Passed all asserts");
}