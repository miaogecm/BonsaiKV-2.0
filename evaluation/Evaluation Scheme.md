# Evaluation Scheme

## Index tier

####    (a) Evaluating Indexing Technique

+ figure 10
+ BonsaiKV (DRAM-only, pipelining)
+ BonsaiKV (data offloading, pipelining)
+ BonsaiKV (data offloading only)

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
+ BonsaiKV-no striping
+ BonsaiKV-SW striping
+ BonsaiKV-HW striping

|      | BW   | Throughput | BW   | Throughput | BW   | Throughput |
| ---- | ---- | ---------- | ---- | ---------- | ---- | ---------- |
| 1    |      |            |      |            |      |            |
| 6    |      |            |      |            |      |            |
| 12   |      |            |      |            |      |            |
| 18   |      |            |      |            |      |            |
| 24   |      |            |      |            |      |            |

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

