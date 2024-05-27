#include <queue>
#include <functional>

/// An object pool for reusing old objects
/// T must be trivially copyable. For instance, a pointer type!
/// Not thread safe, keep thread local
template <typename T>
class ObjectPool {    
private:
    std::queue<T> elements;
    std::function<T()> generator;

public:
    ObjectPool(std::function<T()> gen){
        generator = gen;
    }

    /// Fetch an object from the object pool
    inline T fetch(){
        if (elements.size() == 0){
            return generator();
        } else {
            T first = elements.front();
            elements.pop();
            return first;
        }
    }

    /// Allow an object to return back in circulation
    inline void release(T object){
        elements.push(object);
    }
};