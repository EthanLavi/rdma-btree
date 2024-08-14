#pragma once

#include <cstdint>
#include <random>
#include <remus/rdma/rdma.h>

using namespace remus::rdma;

/// Random seeds with around half of the bits set. 
/// Output space is at least 2x the number of threads to prevent overlaps
static uint32_t seed(){
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist(0, 39);
    uint32_t arr[40] = {2463534242, 2318261108, 2689770536, 4186217900, 2639618674, 4135555716, 1896594581, 4177280317, 2478510474, 3602047420, 3882214040, 154032322, 2797879757, 4165739712, 4105014704, 1752638874, 2838708547, 2531708157, 530608692, 2974239974, 4069141101, 2010153904, 1329470636, 2088033866, 2866862742, 2185350033, 3082432825, 2932971466, 1348648012, 3457442513, 3905963781, 3877125244, 1453965676, 83881019, 1280748377, 3148969384, 3231393822, 470576835, 3388582210, 2740827379};
    return arr[dist(rng)];
}

/// Configuration information
template <class K, int MAX_HEIGHT>
struct alignas(64) node {
private:
    /// Got from synchrobench (p=0.5)
    int get_rand_level() {
        static thread_local uint32_t y = seed();
        y^=(y<<13);
        y^=(y>>17);
        y^=(y<<5);
        uint32_t temp = y;
        uint32_t level = 1;
        while (((temp >>= 1) & 1) != 0) {
            ++level;
        }
        /* 1 <= level < MAX_HEIGHT */
        if (level > MAX_HEIGHT) {
            return (int) MAX_HEIGHT;
        } else {
            return (int) level;
        }
    }

public:
    uint64_t value;
    K key;
    int64_t height; // [0, MAX HEIGHT - 1]
    uint64_t link_level; // [0, MAX HEIGHT] (a lock for helper threads to raise a node)
    rdma_ptr<node> next[MAX_HEIGHT];

    node(K key, uint64_t value) : key(key), value(value) {
        height = (uint64_t) get_rand_level();
        link_level = 0;
        for(int i = 0; i < MAX_HEIGHT; i++){
            next[i] = nullptr;
        }
    }
};