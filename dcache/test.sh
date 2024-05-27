# todo: make real cmake tests
cd ../build/dcache
make -j 4

echo "Mark Pointer Test"
./mark_ptr_test
echo ""

echo "Object Pool Test"
./object_pool_test
echo ""

echo "Cached Pointer Test"
./cached_ptr_test
echo ""

echo "Cache Store Test"
./cache_store_test
echo ""
