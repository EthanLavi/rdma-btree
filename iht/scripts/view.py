import os

# Collect results into the table
table = {
    "mean": [], 
    "mean_2": [],
    "p50": [],
    "p50_2": []
}

i = 0
while True:
    filename = f"dev/exp-stats/node{i}-iht_result.csv"
    i += 1
    if not os.path.exists(filename):
        break
    f = open(filename, "r")
    lines = f.read().strip().split("\n")
    f.close()
    header = lines[0].strip().split(",")
    for line_num in range(1, len(lines)):
        line_data = lines[line_num].strip().split(",")
        for i in range(len(header)):
            if header[i] in list(table.keys()):
                table[header[i]].append(int(float(line_data[i])))

for key in table:
    print(f"{key} = {table[key]}")