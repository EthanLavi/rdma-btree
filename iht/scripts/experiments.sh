trap "exit" INT
# echo "Compiling and doing a preliminary test"
# python launch.py -u esl225 --experiment_name bench --runtype bench --region_size 25 --runtime 1 --op_count 1 --op_distribution=100-0-0 -lb 0 -ub 40000 --thread_count 4 --node_count 10 --qp_per_conn 40 --cache_depth 0
# echo "Done with preliminary tests. Running experiment"
for k in 3
do
    for t in 1 4 8
    do
        for n in 3 4 5 6 7 8 9 10
        do
            python3 launch.py -u esl225 --experiment_name ${t}t_${n}n_${k}k_btree --runtype cached --region_size 28 --runtime 10 --op_count 5000000 --op_distribution=80-10-10 -lb 0 -ub 100000 --thread_count $t --node_count $n --qp_per_conn 60 --cache_depth $k --level info --structure btree --distribution uniform
            python3 launch.py -u esl225 --experiment_name ${t}t_${n}n_${k}k_btree --runtype cached --region_size 28 --runtime 60 --op_count 5000000 --op_distribution=80-10-10 -lb 0 -ub 100000 --thread_count $t --node_count $n --qp_per_conn 60 --cache_depth $k --level info --structure sherman --distribution uniform
        done
    done
done
