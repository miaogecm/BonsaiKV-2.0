# Evaluation Scheme

## Index tier

####    (a) Evaluating Indexing Technique

+ figure 10
+ BonsaiKV (DRAM-only, pipelining)
+ BonsaiKV (data offloading, pipelining)
+ BonsaiKV (data offloading only)

+ throughput (M op/s)
+ pipelining: 4 coroutines

| thread |       |       |      |
| ------ | ----- | ----- | ---- |
| 1      | 1.25  | 1.67  | 0.84 |
| 6      | 7.49  | 6.82  | 2.41 |
| 12     | 9.53  | 8.40  | 3.69 |
| 18     | 12.96 | 10.05 | 4.65 |
| 24     | 14.57 | 13.22 | 6.04 |

+ DRAM
  + same

#### (b) Evaluating Data Offloading technique

+ skewed dataset
+ metrics
  + data offloading throughput
  + write reduction rate

+ BonsaiKV (dnode out-of-place)
+ BonsaiKV (dnode in-place)

| $\alpha$ | Mop/s | WRR % | Mop/s | WRR % |
| -------- | ----- | ----- | ----- | ----- |
| uni      | 12.32 | 0     | 2.23  | 0     |
| 0.90     | 19.45 | 43    | 2.38  | 0     |
| 0.99     | 24.33 | 64    | 2.42  | 0     |
| 1.20     | 25.72 | 82    | 2.37  | 0     |
| 1.50     | 26.16 | 93    | 2.41  | 0     |

## Persistence tier

+ same

## Scalability tier

#### Evaluating data striping technique

+ figure12 (a)
+ 1. BonsaiKV-no striping
+ 2. BonsaiKV-SW striping
+ 3. BonsaiKV-HW striping
+ BW: NVM read BW (MB/s)
+ Throughput: 10Kop/s
+ scan

|      | BW (MB/s) | Throughput |        | BW   | Throughput |         | BW   | Throughput |        |
| ---- | --------- | ---------- | ------ | ---- | ---------- | ------- | ---- | ---------- | ------ |
| 1    | 2573      | 34.8       | 10000  | 3782 | 42.4       | 320000  | 5943 | 70.1       | 10000  |
| 6    | 3206      | 41.4       | 60000  | 4004 | 48.7       | 1920000 | 7126 | 95.4       | 60000  |
| 12   | 4376      | 48.2       | 120000 | 4128 | 52.5       | 3840000 | 8027 | 100.2      | 120000 |
| 18   | 4998      | 50.1       | 180000 | 5074 | 59.2       | 5760000 | 9984 | 127.6      | 180000 |

#### Evaluating coherence protocol overhead

+ figure12 (b)



## Synthesis Benchmark

#### KVs

+ BonsaiKV
+ Clover
+ pactree + infiniswap
+ Sherman (+ persistence)

#### Benchmarks

+ integer KV / string KV
+ YCSB Load/A/B/C/D/E

