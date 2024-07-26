import random

file_name = "../data/KV.txt"
with open(file_name, "w") as f:
    for i in range(10000):
        k = random.randint(1,500)
        v = random.randint(1,10000) 
        f.write("{},{}\n".format(k, v))
    