import random
import numpy as np

num_particles = 8
num_points = 1000

dataset = {}
for p in range(num_points):
    particles_id = random.randint(0, num_particles-1)
    x, y = random.randint(0, 1000), random.randint(0, 1000)
    dataset[(x, y)] = particles_id


dataset = [(particle_id, x, y) for (x, y), particle_id in dataset.items()]

with open("./data/points.csv", "w") as f:
    f.write("particle_id,x,y\n")
    for row in dataset:
        f.write("{},{},{}\n".format(*row))
