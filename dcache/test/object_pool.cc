#include "../include/object_pool.h"

#include <functional>
#include <remus/logging/logging.h>

#define test(condition, message){ \
    if (!(condition)){ \
        REMUS_ERROR("Error: {}", message); \
        exit(1); \
    } \
}

int main(){
    ObjectPool<int> pool = ObjectPool<int>(std::function<int()>([=](){
        static int gen = 1;
        return gen++;
    }));
    
    test(pool.fetch() == 1, "Empty pool calls generate");
    test(pool.fetch() == 2, "Empty pool still calls generate");
    test(pool.empty(), "Pool has items unexpectedly");
    pool.release(10);
    test(!pool.empty(), "Pool is unexpectedly empty");
    test(pool.fetch() == 10, "Release adds back to the pool");
    test(pool.empty(), "Pool has items unexpectedly");
    test(pool.fetch() == 3, "Pool is now empty");
    test(pool.empty(), "Pool has items unexpectedly");

    REMUS_INFO("Test 1 -- PASSED");
    return 0;
}