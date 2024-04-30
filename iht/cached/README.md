./iht/iht_rome_cached --cache_depth 0 --runtime 10 --key_lb 5 --key_ub 1000 --op_count 10000 --qp_max 1 --region_size 22 --contains 80 --insert 10 --remove 10 --node_count 2 --thread_count 1 --node_id 0

./iht/iht_rome_cached --cache_depth 0 --runtime 10 --key_lb 5 --key_ub 1000 --op_count 10000 --qp_max 1 --region_size 22 --contains 80 --insert 10 --remove 10 --node_count 2 --thread_count 1 --node_id 1

cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=1 -DCMAKE_BUILD_TYPE=Debug .

gdb ./iht/iht_rome_cached
run --cache_depth 2 --runtime 10 --key_lb 5 --key_ub 1000 --op_count 10000 --qp_max 1 --region_size 22 --contains 80 --insert 10 --remove 10 --node_count 2 --thread_count 1 --node_id 0

gdb ./iht/iht_rome_cached
run --cache_depth 2 --runtime 10 --key_lb 5 --key_ub 1000 --op_count 10000 --qp_max 1 --region_size 22 --contains 80 --insert 10 --remove 10 --node_count 2 --thread_count 1 --node_id 1
