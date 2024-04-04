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
  + scan for 20ms
    + start from the first dnode


+ dataset:
  + YCSB-E (scan-only)
  + 0.3GB data

| n threads | A: BW (MB/s) | A: Throughput | A: N   | B: BW | B: Throughput | B: N    | C: BW | C: Throughput | C: N   |
| --------- | ------------ | ------------- | ------ | ----- | ------------- | ------- | ----- | ------------- | ------ |
| 1         | 2573         | 34.8          | 5884   | 3782  | 42.4          | 184992  | 5943  | 70.1          | 11885  |
| 6         | 3206         | 41.4          | 32069  | 4004  | 48.7          | 1068045 | 7126  | 95.4          | 73898  |
| 12        | 4376         | 48.2          | 81455  | 4128  | 52.5          | 2500394 | 8027  | 100.2         | 166367 |
| 18        | 4998         | 50.1          | 110041 | 5074  | 59.2          | 3668310 | 9984  | 127.6         | 358338 |

#### Evaluating coherence protocol overhead

+ same as figure12 (b) in VLDB paper

## Synthesis Benchmark

#### KVs

+ BonsaiKV
+ Dinomo
  + client+KN+RN+MN: node0
  + DPM: node1


+ pactree + fastswap
+ Sherman (+ persistence)

```
./benchmark kNodeCount kReadRatio kThreadCount # run sherman benchmark
```

#### Benchmarks

+ integer KV / string KV
+ YCSB Load/A/C/D/E
+ A: 50% read, 50% write
+ C: 100% read
+ D: 95% read, 5% write
+ E: 95% scan, 5% write

#### YCSB

##### YCSB-Load-Int

| NThreads | Sherman |      | PACTree + AIFM |      | DINOMO |      | BonsaiKV |      |
| -------- | ------- | ---- | -------------- | ---- | ------ | ---- | -------- | ---- |
| 1        | 0.59    |      | 0.28           |      | 0.25   |      | 2.04     |      |
| 6        | 0.83    |      | 0.62           |      | 0.62   |      | 9.83     |      |
| 12       | 1.24    |      | 1.08           |      | 1.13   |      | 12.23    |      |
| 18       | 2.02    |      | 2.13           |      | 1.25   |      | 19.84    |      |
| 24       | 2.94    |      | 2.56           |      | 1.78   |      | 25.06    |      |
| 30       | 3.55    |      | 2.98           |      | 1.98   |      | 28.43    |      |
| 36       | 4.30    |      | 3.27           |      | 2.35   |      | 30.02    |      |

+ number of round trips
  + sherman: 1 (inner node cached in DRAM, only one access to remote node)
  + pactree + aifm: 1 (inner node hot, cached in DRAM)
  + dinomo: 5 (client -> RN -> KN -> DM (twice))
  + BonsaiKV: 1

##### YCSB-A-Int

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV |
| -------- | ------- | -------------- | ------ | -------- |
| 1        | 0.23    | 0.32           | 0.19   | 1.65     |
| 6        | 1.44    | 0.95           | 0.56   | 7.78     |
| 12       | 2.18    | 1.94           | 0.94   | 10.23    |
| 18       | 3.17    | 2.97           | 1.28   | 13.74    |
| 24       | 4.18    | 3.42           | 1.62   | 15.83    |
| 30       | 5.04    | 3.98           | 2.07   | 16.31    |
| 36       | 5.73    | 4.27           | 2.34   | 18.52    |

##### YCSB-C-Int

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV |
| -------- | ------- | -------------- | ------ | -------- |
| 1        | 0.34    | 0.26           | 0.22   | 1.12     |
| 6        | 1.97    | 0.93           | 0.61   | 3.27     |
| 12       | 4.02    | 3.04           | 1.27   | 6.82     |
| 18       | 5.31    | 5.03           | 1.46   | 8.04     |
| 24       | 6.24    | 5.95           | 1.82   | 13.09    |
| 30       | 8.32    | 7.01           | 2.23   | 14.73    |
| 36       | 8.95    | 7.47           | 2.60   | 15.02    |

+ IO size: (disadvantages of sherman and pactree)
  + sherman: 1024
  + PACTree + AIFM: 1232
  + DINIMO: 72
  + BonsaiKV: 16

##### YCSB-D-Int

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV |
| -------- | ------- | -------------- | ------ | -------- |
| 1        | 0.28    | 0.24           | 0.27   | 1.07     |
| 6        | 1.70    | 0.82           | 0.59   | 2.95     |
| 12       | 2.84    | 3.15           | 1.35   | 6.74     |
| 18       | 4.47    | 5.22           | 1.57   | 10.28    |
| 24       | 5.96    | 5.48           | 1.76   | 12.85    |
| 30       | 7.45    | 6.46           | 2.31   | 13.44    |
| 36       | 7.96    | 6.87           | 2.72   | 14.93    |

##### YCSB-E-Int

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV |
| -------- | ------- | -------------- | ------ | -------- |
| 1        | 0.08    | 0.03           | /      | 0.35     |
| 6        | 0.12    | 0.09           | /      | 0.41     |
| 12       | 0.28    | 0.19           | /      | 0.48     |
| 18       | 0.33    | 0.25           | /      | 0.52     |
| 24       | 0.45    | 0.34           | /      | 0.62     |
| 30       | 0.57    | 0.49           | /      | 0.68     |
| 36       | 0.62    | 0.55           | /      | 0.71     |

##### YCSB-Load-String

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV | BonsaiKV - SP |
| -------- | ------- | -------------- | ------ | -------- | ------------- |
| 1        |         |                |        | 0.05     | same          |
| 6        |         |                |        | 0.42     | as            |
| 12       |         |                |        | 0.79     | before        |
| 18       |         |                |        | 0.85     |               |
| 24       |         |                |        | 0.89     |               |
| 30       |         |                |        | 0.97     |               |
| 36       |         |                |        | 1.21     |               |

##### YCSB-A-String

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV |
| -------- | ------- | -------------- | ------ | -------- |
| 1        |         |                |        | 0.27     |
| 6        |         |                |        | 1.08     |
| 12       |         |                |        | 2.53     |
| 18       |         |                |        | 2.63     |
| 24       |         |                |        | 3.20     |
| 30       |         |                |        | 3.64     |
| 36       |         |                |        | 3.92     |

##### YCSB-C-String

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV | BonsaiKV-replica |
| -------- | ------- | -------------- | ------ | -------- | ---------------- |
| 1        |         |                |        | 0.42     | 0.48             |
| 6        |         |                |        | 1.85     | 1.92             |
| 12       |         |                |        | 3.46     | 3.49             |
| 18       |         |                |        | 4.35     | 4.39             |
| 24       |         |                |        | 5.76     | 4.28             |
| 30       |         |                |        | 6.28     | 5.34             |
| 36       |         |                |        | 6.64     | 6.02             |

##### YCSB-D-String

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV |
| -------- | ------- | -------------- | ------ | -------- |
| 1        |         |                |        | 0.38     |
| 6        |         |                |        | 1.79     |
| 12       |         |                |        | 3.40     |
| 18       |         |                |        | 4.28     |
| 24       |         |                |        | 5.79     |
| 30       |         |                |        | 6.04     |
| 36       |         |                |        | 6.53     |

##### YCSB-E-String

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV |
| -------- | ------- | -------------- | ------ | -------- |
| 1        |         |                |        | 0.12     |
| 6        |         |                |        | 0.23     |
| 12       |         |                |        | 0.24     |
| 18       |         |                |        | 0.32     |
| 24       |         |                |        | 0.30     |
| 30       |         |                |        | 0.34     |
| 36       |         |                |        | 0.42     |

