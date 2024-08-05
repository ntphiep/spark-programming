import random

rdd_1 = "./data/compare/ex1.txt"
rdd_2 = "./data/compare/ex2.txt"

num = 10_000_000

with open(rdd_1, "w") as file:
    for i in range(num):
        k = random.randint(1, num)
        v = f"Value_1:{k}"
        file.write("{},{}\n".format(k, v))

with open(rdd_2, "w") as file:
    for i in range(num):
        k = random.randint(1, num)
        v = f"Value_2:{k}"
        file.write("{},{}\n".format(k, v))

if __name__ == "__main__":
    pass
