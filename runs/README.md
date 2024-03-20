# 2024-03-20

## Inputs: 1000, Queries 20

| solution             |   preproc_time |   run_time |
|:---------------------|---------------:|-----------:|
| solution-rust-tree   |       1.00421  |   0.004545 |
| solution-incremental |       1.00422  |   0.012204 |
| solution-flask       |       1.00435  |   0.065973 |
| barab-virag-3        |       2.00805  |   0.083352 |
| solution-aron-mark   |       0.087197 |   0.137765 |
| solution-1           |       1e-06    |   0.368246 |
| barab-virag-2        |       1.10871  |   0.538055 |
| barab-virag-1        |       1.10996  |   0.546926 |

## Inputs: 10000, Queries 50

| solution             |   preproc_time |   run_time |
|:---------------------|---------------:|-----------:|
| solution-rust-tree   |        1.00412 |   0.005059 |
| solution-incremental |        1.00404 |   0.011501 |
| barab-virag-3        |        2.00799 |   0.098922 |
| solution-flask       |        1.00426 |   0.136881 |
| solution-aron-mark   |        0.09517 |   0.193551 |
| barab-virag-2        |        1.66432 |   0.525454 |
| barab-virag-1        |        1.72553 |   0.70755  |

## Inputs: 50000, Queries 200

| solution             |   preproc_time |   run_time |
|:---------------------|---------------:|-----------:|
| solution-rust-tree   |       1.00436  |   0.005557 |
| solution-incremental |       1.00772  |   0.029761 |
| barab-virag-3        |       4.01116  |   0.234382 |
| solution-aron-mark   |       0.100059 |   0.675314 |
| solution-flask       |       1.00619  |   0.698467 |
| barab-virag-2        |       3.8672   |   0.799963 |
| barab-virag-1        |       3.92633  |   1.57432  |

## Inputs: 250000, Queries 500

| solution             |   preproc_time |   run_time |
|:---------------------|---------------:|-----------:|
| solution-rust-tree   |       1.00432  |   0.007755 |
| solution-incremental |       1.00647  |   0.047335 |
| barab-virag-3        |      16.0309   |   0.618453 |
| barab-virag-2        |      15.555    |   1.1782   |
| solution-aron-mark   |       0.113246 |   3.24674  |
| barab-virag-1        |      15.5561   |   4.42755  |
| solution-flask       |       1.00684  |   8.14385  |

## Inputs: 1000000, Queries 1000

| solution             |   preproc_time |   run_time |
|:---------------------|---------------:|-----------:|
| solution-rust-tree   |        1.00649 |   0.012977 |
| solution-incremental |        1.0062  |   0.278018 |
| barab-virag-3        |       63.1175  |   1.35593  |
| barab-virag-2        |       62.4038  |   2.16364  |

## Inputs: 10000000, Queries 1000

| solution             |   preproc_time |   run_time |
|:---------------------|---------------:|-----------:|
| solution-rust-tree   |        5.01611 |   0.014845 |
| barab-virag-3        |      683.248   |   1.19834  |
| barab-virag-2        |      686.982   |   2.61088  |
| solution-incremental |        6.02535 |   6.72727  |