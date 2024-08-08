import re

def extract_spark_log_info(log_file):
    info = {
        "spark_version": None,
        "os_info": None,
        "java_version": None,
        "num_partitions": None,
        "block_manager_info": [],
    }

    with open(log_file, 'r') as f:
        lines = f.readlines()

    for line in lines:
        if "Spark version" in line:
            info["spark_version"] = line.strip().split("Spark version ")[1]
        elif "OS info" in line:
            info["os_info"] = line.strip().split("OS info ")[1]
        elif "Java version" in line:
            info["java_version"] = line.strip().split("Java version ")[1]
        elif "Number of partitions of" in line:
            info["num_partitions"] = int(re.search(r'(\d+)', line).group(0))
        elif "BlockManagerId(" in line:
            bm_info = line.strip()
            info["block_manager_info"].append(bm_info)

    return info

# Đường dẫn tới file log
log_file = "log.txt"

# Trích xuất thông tin
log_info = extract_spark_log_info(log_file)

# In ra các thông tin hữu ích
print("Spark Version:", log_info["spark_version"])
print("OS Info:", log_info["os_info"])
print("Java Version:", log_info["java_version"])
print("Number of Partitions:", log_info["num_partitions"])
print("BlockManager Info:")
for bm in log_info["block_manager_info"]:
    print(bm)
