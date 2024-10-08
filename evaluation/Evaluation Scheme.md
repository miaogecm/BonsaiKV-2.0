# Evaluation Scheme

## Motivation

### DLP: Latency

+ read-only, 2K IO
+ us

| thread | Frontend | Backend: 1 | Backend: 2 | Backend: 3 |
| ------ | -------- | ---------- | ---------- | ---------- |
| 8      | 2.87     | 1.26       | 0.42       | 0.20       |
| 16     | 3.31     | 2.01       | 0.95       | 0.28       |
| 24     | 4.32     | 3.76       | 1.17       | 0.33       |
| 28     | 4.97     | 5.11       | 1.17       | 0.31       |
| 32     | 5.42     | 5.82       | 1.90       | 0.35       |
| 36     | 6.62     | 6.46       | 2.13       | 0.32       |

### DLP: BW

+ R:W=9:1, 8K IO
+ 200Gbps network
+ Net: use mellanox hardware counter (rx_read_requests)
+ PCIe: use pcm-iio
+ NVM: use pcm-memory

| n DIMMs    | Net (MB/s)  | PCIe (MB/s) | NVM (MB/s) |
| ---------- | ----------- | ----------- | ---------- |
| 1 (4GB/s)  | 3902 (16%)  | 3987 (12%)  | 4041       |
| 2 (8GB/s)  | 7598 (30%)  | 7622 (23%)  | 7822       |
| 3 (12GB/s) | 11028 (44%) | 11208 (34%) | 11244      |
| 4 (16GB/s) | 15184 (61%) | 15264 (46%) | 15368      |
| 5 (20GB/s) | 19882 (80%) | 19893 (62%) | 19942      |
| 6 (24GB/s) | 23705 (95%) | 23812 (74%) | 23867      |

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

| NThreads | Sherman |      | PACTree + AIFM |      | DINOMO |      | BonsaiKV |      | BonsaiKV-old |
| -------- | ------- | ---- | -------------- | ---- | ------ | ---- | -------- | ---- | ------------ |
| 1        | 0.59    |      | 0.28           |      | 0.25   |      | 2.04     |      | 1.38         |
| 6        | 0.83    |      | 0.62           |      | 0.62   |      | 9.83     |      | 5.25         |
| 12       | 1.24    |      | 1.08           |      | 1.13   |      | 12.23    |      | 7.82         |
| 18       | 2.02    |      | 2.13           |      | 1.25   |      | 19.84    |      | 10.07        |
| 24       | 2.94    |      | 2.56           |      | 1.78   |      | 25.06    |      | 12.24        |
| 30       | 3.55    |      | 2.98           |      | 1.98   |      | 28.43    |      | 15.48        |
| 36       | 4.30    |      | 3.27           |      | 2.35   |      | 30.02    |      | 17.53        |

+ number of round trips
  + sherman: 1 (inner node cached in DRAM, only one access to remote node)
  + pactree + aifm: 1 (inner node hot, cached in DRAM)
  + dinomo: 5 (client -> RN -> KN -> DM (twice))
  + BonsaiKV: 0
+ IO size: (disadvantages of sherman and pactree)

  + sherman: 1024
  + PACTree + AIFM: 1232
  + DINIMO: 40
  + BonsaiKV: 0

##### YCSB-A-Int

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV | BonsaiKV-old |
| -------- | ------- | -------------- | ------ | -------- | ------------ |
| 1        | 0.23    | 0.32           | 0.19   | 1.65     | 0.49         |
| 6        | 1.44    | 0.95           | 0.56   | 7.78     | 3.67         |
| 12       | 2.18    | 1.94           | 0.94   | 10.23    | 5.83         |
| 18       | 3.17    | 2.97           | 1.28   | 13.74    | 8.25         |
| 24       | 4.18    | 3.42           | 1.62   | 15.83    | 8.92         |
| 30       | 5.04    | 3.98           | 2.07   | 16.31    | 9.07         |
| 36       | 5.73    | 4.27           | 2.34   | 18.52    | 10.45        |

##### YCSB-C-Int

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV | BonsaiKV-old |
| -------- | ------- | -------------- | ------ | -------- | ------------ |
| 1        | 0.34    | 0.26           | 0.22   | 1.12     | 0.84         |
| 6        | 1.97    | 0.93           | 0.61   | 3.27     | 2.34         |
| 12       | 4.02    | 3.04           | 1.27   | 6.82     | 4.50         |
| 18       | 5.31    | 5.03           | 1.46   | 8.04     | 5.99         |
| 24       | 6.24    | 5.95           | 1.82   | 13.09    | 9.84         |
| 30       | 8.32    | 7.01           | 2.23   | 14.73    | 10.56        |
| 36       | 8.95    | 7.47           | 2.60   | 15.02    | 10.49        |

+ IO size: (disadvantages of sherman and pactree)
  + sherman: 1024
  + PACTree + AIFM: 1232
  + DINIMO: 72
  + BonsaiKV: 16

##### YCSB-D-Int

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV | BonsaiKV-old |
| -------- | ------- | -------------- | ------ | -------- | ------------ |
| 1        | 0.28    | 0.24           | 0.27   | 1.07     | 0.79         |
| 6        | 1.70    | 0.82           | 0.59   | 2.95     | 1.98         |
| 12       | 2.84    | 3.15           | 1.35   | 6.74     | 4.32         |
| 18       | 4.47    | 5.22           | 1.57   | 10.28    | 5.65         |
| 24       | 5.96    | 5.48           | 1.76   | 12.85    | 8.97         |
| 30       | 7.45    | 6.46           | 2.31   | 13.44    | 9.94         |
| 36       | 7.96    | 6.87           | 2.72   | 14.93    | 10.25        |

##### YCSB-E-Int

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV | BonsaiKV-old |
| -------- | ------- | -------------- | ------ | -------- | ------------ |
| 1        | 0.08    | 0.03           | /      | 0.35     | 0.12         |
| 6        | 0.12    | 0.09           | /      | 0.41     | 0.23         |
| 12       | 0.28    | 0.19           | /      | 0.48     | 0.31         |
| 18       | 0.33    | 0.25           | /      | 0.52     | 0.34         |
| 24       | 0.45    | 0.34           | /      | 0.62     | 0.40         |
| 30       | 0.57    | 0.49           | /      | 0.68     | 0.46         |
| 36       | 0.62    | 0.55           | /      | 0.71     | 0.49         |

##### YCSB-Load-String

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV | BonsaiKV - SP | old  |
| -------- | ------- | -------------- | ------ | -------- | ------------- | ---- |
| 1        | 0.13    | 0.16           | 0.08   | 0.05     | same          | 0.05 |
| 6        | 0.48    | 0.44           | 0.34   | 0.42     | as            | 0.39 |
| 12       | 0.69    | 0.62           | 0.54   | 0.79     | before        | 0.62 |
| 18       | 0.80    | 0.75           | 0.62   | 0.85     |               | 0.75 |
| 24       | 0.79    | 0.83           | 0.67   | 0.89     |               | 0.80 |
| 30       | 0.96    | 0.89           | 0.84   | 0.97     |               | 0.84 |
| 36       | 0.94    | 0.92           | 0.86   | 1.21     |               | 0.95 |

##### YCSB-A-String

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV | old  |
| -------- | ------- | -------------- | ------ | -------- | ---- |
| 1        | 0.18    | 0.20           | 0.15   | 0.27     | 0.19 |
| 6        | 1.04    | 1.03           | 0.94   | 1.08     | 0.99 |
| 12       | 2.04    | 2.09           | 1.89   | 2.53     | 1.92 |
| 18       | 2.23    | 2.42           | 2.13   | 2.63     | 2.34 |
| 24       | 2.30    | 2.51           | 2.42   | 3.20     | 2.53 |
| 30       | 2.59    | 2.69           | 2.48   | 3.64     | 2.88 |
| 36       | 2.91    | 3.04           | 2.55   | 3.92     | 3.04 |

##### YCSB-C-String

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV | BonsaiKV-replica | old  |
| -------- | ------- | -------------- | ------ | -------- | ---------------- | ---- |
| 1        | 0.36    | 0.37           | 0.32   | 0.42     | 0.48             | 0.42 |
| 6        | 1.62    | 1.69           | 1.54   | 1.85     | 1.92             | 1.85 |
| 12       | 3.35    | 3.29           | 3.02   | 3.46     | 3.49             | 3.07 |
| 18       | 4.28    | 4.34           | 4.12   | 4.35     | 4.39             | 3.98 |
| 24       | 4.31    | 4.36           | 4.15   | 5.76     | 5.04             | 4.52 |
| 30       | 4.53    | 4.62           | 4.49   | 6.28     | 5.34             | 4.89 |
| 36       | 4.79    | 4.73           | 4.53   | 6.64     | 6.02             | 5.34 |

##### YCSB-D-String

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV | old  |
| -------- | ------- | -------------- | ------ | -------- | ---- |
| 1        | 0.32    | 0.38           | 0.29   | 0.38     | 0.40 |
| 6        | 1.54    | 1.64           | 1.56   | 1.79     | 1.76 |
| 12       | 3.08    | 3.17           | 2.98   | 3.40     | 3.27 |
| 18       | 4.17    | 4.29           | 3.94   | 4.28     | 3.65 |
| 24       | 4.32    | 4.33           | 4.03   | 5.79     | 4.39 |
| 30       | 4.49    | 4.58           | 4.34   | 6.04     | 4.52 |
| 36       | 4.75    | 4.84           | 4.52   | 6.53     | 4.99 |

##### YCSB-E-String

| NThreads | Sherman | PACTree + AIFM | DINOMO | BonsaiKV | old  |
| -------- | ------- | -------------- | ------ | -------- | ---- |
| 1        | 0.09    | 0.13           | /      | 0.12     | 0.10 |
| 6        | 0.15    | 0.17           | /      | 0.23     | 0.19 |
| 12       | 0.18    | 0.16           | /      | 0.24     | 0.21 |
| 18       | 0.23    | 0.21           | /      | 0.32     | 0.28 |
| 24       | 0.25    | 0.24           | /      | 0.38     | 0.29 |
| 30       | 0.27    | 0.29           | /      | 0.42     | 0.34 |
| 36       | 0.31    | 0.33           | /      | 0.48     | 0.37 |

