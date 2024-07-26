import random

file_name_1 = "./data/ex1.txt"
file_name_2 = "./data/ex2.txt"


with open(file_name_1, "w") as file:
    for i in range(10000):
        k = random.randint(0, 500)
        v = random.randint(1, 10000) 
        file.write("{},{}\n".format(k, v))

with open(file_name_2, "w") as file:
    rand_numbers = [random.randint(1, 1_000_000) for _ in range(10_000_000)]
    [file.write(f"{i}\n") for i in rand_numbers]