#include <atomic>

using namespace std;

/// Single Producer Single Consumer Queue (spscq)
/// Taken from the book
template <class T>
class ConcurrentQueue {
    atomic<int> start;

    ConcurrentQueue(int buffer_size){

    }

    T front(){

    }

    void pop(){

    }

    void push(T obj){

    }
};