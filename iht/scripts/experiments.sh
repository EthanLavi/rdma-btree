trap "exit" INT
# echo "Compiling and doing a preliminary test"
# python launch.py -u esl225 --experiment_name bench --runtype bench --region_size 25 --runtime 1 --op_count 1 --op_distribution=100-0-0 -lb 0 -ub 40000 --thread_count 4 --node_count 10 --qp_max 40 --cache_depth 0
echo "Done with preliminary tests. Running experiment"
for k in 3
do
    for t in 4
    do
        for n in 3 4 5 6 7 8 9 10
        do
            python3 launch.py -u esl225 --experiment_name ${t}t_${n}n_${k}k_two_sided --runtype twosided --region_size 25 --runtime 60 --unlimited_stream --op_distribution=80-10-10 -lb 0 -ub 100000 --thread_count $t --node_count $n --qp_max 60 --cache_depth $k --level info
            # python3 launch.py -u esl225 --experiment_name tmp --runtype bench --region_size 25 --runtime 60 --unlimited_stream --op_distribution=80-10-10 -lb 0 -ub 40000 --thread_count 10 --node_count 2 --qp_max 40 --cache_depth $k --devmode
        done
    done
done
