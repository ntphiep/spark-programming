# Spark Optimization

Table of Contents
- [Spark Optimization](#spark-optimization)
  - [1. `repartition` vs `coalesce`](#1-repartition-vs-coalesce)
  - [2. Hash Partitioning vs Range Partitioning on Skewed Data](#2-hash-partitioning-vs-range-partitioning-on-skewed-data)
  - [3. Memory Management in Spark](#3-memory-management-in-spark)


## 1. `repartition` vs `coalesce`

- `repartition`: Increase or decrease the number of partitions in a DataFrame.
  - It shuffles the data to create equal-sized partitions.
  - It is expensive as it involves shuffling the data.
  - It can be used to increase the number of partitions.
  - Used when you want to increase the number of partitions or distribute data across partitions (if the current partitioning is skewed or not optimal).

- `coalesce`: Decrease the number of partitions in a DataFrame.
  - It does not shuffle the data.
  - It is less expensive than `repartition`.
  - It can be used to decrease the number of partitions.
  - Used when you want to decrease the number of partitions (if the current partitioning is not optimal).

> **Note**: Skewness in the data can lead to uneven partitions. In such cases, `repartition` can be used to create equal-sized partitions.

## 2. Hash Partitioning vs Range Partitioning on Skewed Data

Skewed data can lead to uneven partitions, that can affect the performance of the Spark job. In such cases, we can use hash partitioning or range partitioning to re-distribute the data across partitions.

- **Hash Partitioning**: 
  - Partition the data based on a hash function. Assign each record to a partition based on the hash value of a column.
  - Used hash when you want to distribute the data uniformly across partitions.
  - Particular useful for operations like `join`, `groupBy` where the data needs to be shuffled across partitions.
  
- **Range Partitioning**:
  - Partition the data based on a range of values. Assign each record to a partition based on the range of values of a column.
  - Used range when you want to partition the data based on a specific column value.
  - Useful when you want to partition the data based on a specific column value (e.g., partition the data based on date range).

> **Note**: Hash partitioning is useful when you want to distribute the data uniformly across partitions, while range partitioning is useful when you want to partition the data based on a specific column value.


## 3. Memory Management in Spark

Spark provides various memory management options to optimize the performance of the Spark job. Some of the key memory management options are:

- **Memory Fraction**: 
  - The fraction of the JVM heap space that is allocated to Spark.
  - It is controlled by the `spark.memory.fraction` property.
  - It determines the amount of memory allocated to execution and storage.

- **Storage Memory Fraction**:
  - The fraction of the memory allocated to storage (caching and shuffling).
  - It is controlled by the `spark.memory.storageFraction` property.
  - It determines the amount of memory allocated to caching and shuffling.

- **Execution Memory Fraction**:
  - The fraction of the memory allocated to execution (task execution and join operations).
  - It is controlled by the `spark.memory.executionFraction` property.
  - It determines the amount of memory allocated to task execution and join operations.

- **Off-Heap Memory**:
  - The memory allocated outside the JVM heap space.
  - It is controlled by the `spark.memory.offHeap.enabled` property.
  - It can be used to allocate memory outside the JVM heap space for better memory management.
  - It can be useful when the JVM heap space is not sufficient for the Spark job.

- **Memory Overhead**:
  - The amount of memory reserved for internal data structures and other memory overheads.
  - It is controlled by the `spark.memory.offHeap.size` property.
  - It determines the amount of memory reserved for internal data structures and other memory overheads.