import os
import matplotlib.pyplot as plt
from typing import List
import sys

if len(sys.argv) == 1:
    print("Missing arg1 = threads")
    exit(0)
if len(sys.argv) == 2:
    print("Missing arg2 = filename")
    exit(0)
if len(sys.argv) != 3:
    print("Too many args")
    exit(0)

def mean(arr):
    summ = 0
    for a in arr:
        summ += a
    return summ / len(arr)

# Configuration parameters
result_directory = "results"
threads = int(sys.argv[1])
files = [[f"{threads}t_2n_3k_one_sided-stats", 
         f"{threads}t_3n_3k_one_sided-stats", 
         f"{threads}t_4n_3k_one_sided-stats", 
         f"{threads}t_5n_3k_one_sided-stats", 
         f"{threads}t_6n_3k_one_sided-stats", 
         f"{threads}t_7n_3k_one_sided-stats", 
         f"{threads}t_8n_3k_one_sided-stats", 
         f"{threads}t_9n_3k_one_sided-stats",
         f"{threads}t_10n_3k_one_sided-stats"],
         [f"{threads}t_2n_3k_two_sided-stats", 
         f"{threads}t_3n_3k_two_sided-stats", 
         f"{threads}t_4n_3k_two_sided-stats", 
         f"{threads}t_5n_3k_two_sided-stats", 
         f"{threads}t_6n_3k_two_sided-stats", 
         f"{threads}t_7n_3k_two_sided-stats", 
         f"{threads}t_8n_3k_two_sided-stats", 
         f"{threads}t_9n_3k_two_sided-stats",
         f"{threads}t_10n_3k_two_sided-stats"]]
nodes = [2, 3, 4, 5, 6, 7, 8, 9, 10]
title = "IHT Client Throughput Graph"
subtitle = f"(r320 [mlnx-4], {threads} threads, 60 seconds runtime)"
x_label = "Clients"
outfile = sys.argv[2] + ".png"
format_legend = ["One-sided", "Two-sided"] # can use () to add a argument
group_difference = "cache_depth"

# Get the results
xs = []
ys = []
top_y = 0
legend_labels = []
for group in files:
    # iterate through each group
    group_data = []
    for i, file in enumerate(group):
        # iterate through each directory in the group
        stats_dir = os.path.join(result_directory, file)
        num_nodes = nodes[i]
        csv_files = os.listdir(stats_dir)
        data = {}
        total_rows = 0
        for n in range(num_nodes):
            # iterate through each file in the directory
            csv_file = f"node{n}-iht_result.csv"
            if csv_file not in csv_files:
                print(csv_file, file)
                print("Error graphing")
                exit(0)
            csv_file = os.path.join(stats_dir, csv_file)
            f = open(csv_file, "r")
            header = f.readline().strip().split(",")
            if header[-1] == "":
                # remove the last value if its empty (trailing comma)
                header.pop()
            for row in f.readlines():
                # iterate through the rows in the file
                total_rows += 1
                values = row.strip().split(",")
                for i, value in enumerate(values):
                    if value == "":
                        # ignore any empty values from trailing commas 
                        continue
                    # iterate through the values in the row
                    if header[i] not in data:
                        data[header[i]] = 0
                    if value.replace(".", "").isnumeric():
                        # if is numeric, add it in the map
                        data[header[i]] += float(value)
                    else:
                        # otherwise just set it into the map
                        data[header[i]] = value
        # After finishing the directory
        for key in data:
            if type(data[key]) != str:
                data[key] = data[key] / total_rows
        # remove unused columns to make development easier
        smaller_data = {}
        for key in data:
            if key in ["thread_count", "node_count", "cache_depth", "units", "mean"]:
                smaller_data[key] = data[key]
        group_data.append(smaller_data)
    # Process group data
    x = []
    y = []
    id = group_data[0][group_difference]
    for i, exp in enumerate(group_data):
        print(exp)
        # make sure the numbers are as expected
        if int(exp['node_count']) != nodes[i] or exp[group_difference] != id:
            print(group)
            print(i)
            print(group[i])
        assert(int(exp['node_count']) == nodes[i])
        assert(exp[group_difference] == id)
        x.append(nodes[i])
        y.append(exp['mean'] * exp['node_count'] * exp['thread_count'])
    legend_labels.append(str(int(id)))
    xs.append(x)
    ys.append(y)
    top_y = max(top_y, max(y))

# Draw the graph
fig = plt.figure(figsize=(8, 8))
plt.suptitle(title)
plt.title(subtitle)
locs, labels = plt.xticks()
plt.setp(labels, rotation=45)
plt.xlabel(x_label)
plt.ylabel("Total Throughput (ops)")
colors = ['red', 'blue', 'green', 'purple', 'orange', 'yellow']
for i in range(len(xs)):
    plt.plot(xs[i], ys[i], ".-", color=colors[i], markersize=15, label=format_legend[i].replace("()", legend_labels[i]))
plt.legend()
plt.ylim(0, top_y * 1.1)
plt.savefig(outfile)
plt.show()
