# Evaluation Scheme

## Index tier

####    (a) Evaluating Indexing Technique

+ example: figure 10

+ metrics: throughput (M op/s)
+ setup:
  + pipelining: use 4 coroutines (optimal)
+ dataset:
  + YCSB-C (read-only)
  + 0.3GB data

| thread | DRAM-only, pipelining | data offloading + metadata uploading + pipelining | data offloading + pipelining | data offloading only |
| ------ | --------------------- | ------------------------------------------------- | ---------------------------- | -------------------- |
| 1      | 2.81                  | 2.98                                              | 1.67                         | 0.84                 |
| 6      | 8.27                  | 7.49                                              | 6.82                         | 2.41                 |
| 12     | 14.53                 | 13.25                                             | 8.40                         | 3.69                 |
| 18     | 16.84                 | 15.03                                             | 10.05                        | 4.65                 |
| 24     | 20.22                 | 19.17                                             | 13.22                        | 6.04                 |

+ DRAM consomption
  + same as VLDB paper

#### (b) Evaluating Pipelining

+ metrics: 
  + throughput (Mop/s)
  + NVM BW (GB/s)
  + IB BW (Gbps)
+ setup:
  + 24 threads
+ dataset:
  + YCSB-C (read only)
  + 0.3GB data

|                 | throughput | NVM (GB/s) | IB (Gbps) |
| --------------- | ---------- | ---------- | --------- |
| no pipelining   | 6.04       | 0.18       | 1.48      |
| intra only      | 7.15       | 0.42       | 1.54      |
| intra + 2 inter | 10.83      | 1.37       | 2.02      |
| intra + 4 inter | 13.22      | 2.62       | 2.4       |
| intra + 8 inter | 12.87      | 1.94       | 2.2       |

#### (c) Evaluating Data Offloading technique

+ setup:
  + 1 thread

+ metrics:
  + data offloading throughput (Mop/s)
  + write reduction rate
+ dataset:
  + YCSB-update-only
  + skewed dataset (zipf dist)
  + 0.3GB data

+ comparison:
  + A: BonsaiKV (bnode in-place merge + dnode out-of-place update)
  + B: BonsaiKV (dnode in-place update, naive solution)

| $\alpha$ | A: Mop/s | A: WRR % | B: Mop/s | B: WRR % |
| -------- | -------- | -------- | -------- | -------- |
| uni      | 12.32    | 0        | 2.23     | 0        |
| 0.90     | 19.45    | 43       | 2.38     | 0        |
| 0.99     | 24.33    | 64       | 2.42     | 0        |
| 1.20     | 25.72    | 82       | 2.37     | 0        |
| 1.50     | 26.16    | 93       | 2.41     | 0        |

## Persistence tier

+ same as VLDB paper

## Scalability tier

#### Evaluating data striping technique

+ metrics:
  + NVM read bandwidth (MB/s)
  + throughput (10kop/s)
  + number of network reqs

+ example: figure12 (a)
+ comparison:
  + A: BonsaiKV-no striping
  + B: BonsaiKV-SW striping
  + C: BonsaiKV-HW striping
+ setup:
  + 1 thread
  + scan for 20ms


+ dataset:
  + YCSB-E (scan-only)
  + 0.3GB data

|      | A: BW (MB/s) | A: Throughput | A: N   | B: BW | B: Throughput | B: N    | C: BW | C: Throughput | C: N   |
| ---- | ------------ | ------------- | ------ | ----- | ------------- | ------- | ----- | ------------- | ------ |
| 1    | 2573         | 34.8          | 5884   | 3782  | 42.4          | 184992  | 5943  | 70.1          | 11885  |
| 6    | 3206         | 41.4          | 32069  | 4004  | 48.7          | 1068045 | 7126  | 95.4          | 73898  |
| 12   | 4376         | 48.2          | 81455  | 4128  | 52.5          | 2500394 | 8027  | 100.2         | 166367 |
| 18   | 4998         | 50.1          | 110041 | 5074  | 59.2          | 3668310 | 9984  | 127.6         | 358338 |

#### Evaluating coherence protocol overhead

+ same as figure12 (b) in VLDB paper

## Synthesis Benchmark

#### KVs

+ BonsaiKV
+ Clover
+ pactree + infiniswap
+ Sherman (+ persistence)

#### Benchmarks

+ integer KV / string KV
+ YCSB Load/A/B/C/D/E

