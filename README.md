# CoFi-FaaS: A Cost Model for Bloom Filter Pushdown in Serverless Joins

CoFi-FaaS is a cost–time model for serverless query plans with Bloom-filter (BF) pushdown: it estimates resource demand `R` and projects it into the processing time `T` and the monetary cost `C` of a plan, and uses these estimates to choose, per query, which Bloom filters to build and how to size and parallelize each stage. All experiments run TPC-H SF100 (19 join queries) on BLOOM-FaaS deployed on Grid'5000.

## Contents

- [Architecture](#architecture)
- [Technical report](#technical-report)
- [Repository layout](#repository-layout)
- [Evaluation](#evaluation)
- [Run](#run)
- [Acknowledgments](#acknowledgments)

## Architecture

CoFi-FaaS sits on top of **BLOOM-FaaS**, a serverless join framework with distributed Bloom-filter early filtering.

| Module | Role |
|---|---|
| [`model.py`](Cofi/model.py) | Cost–time equations, demand extraction from run logs, calibration of `κ` (NNLS / least squares), prediction of `T` and `C` |
| [`bloom.py`](Cofi/bloom.py) | Bloom-filter accounting: semi-join selectivity, pass rate `φ`, extra BF requests `R_j` |
| [`partition.py`](Cofi/partition.py) | Partitioning of Parquet row groups into function tasks (`max_size_mb`) |
| [`plans.py`](Cofi/plans.py) | Plan synthesis and search, stage resizing, Pareto front, knee selection |
| [`accuracy.py`](Cofi/accuracy.py) | Leave-one-query-out evaluation of the model against measured runs |
| [`components.py`](Cofi/components.py) | Per-stage / per-function estimated vs. measured breakdown |
| [`style.py`](Cofi/style.py) | Shared plotting style (Linux Libertine, cost in ¢) |

## Technical report

[`BLOOM-FaaS Aug 21 Full (Technical report).pdf`](BLOOM-FaaS%20Aug%2021%20Full%20(Technical%20report).pdf)
describes the base system used to run every experiment of this repository (architecture, distributed BF
construction with self-calibration, two-tier storage, and its comparison with Starling and Spark).

> Phan-An-Truong Tran, Laurent d'Orazio, Le Gruenwald, Thuong-Cang Phan. *BLOOM-FaaS: Early Filtering For
> Serverless Joins Using Distributed Bloom Filters.* Preprint, 2026. [hal-05722983](https://hal.science/hal-05722983v1)

## Repository layout

Each section has a notebook, `data/` (input data) and `figures/` (output figures, PDF + PNG).

| Folder | Content |
|---|---|
| [`1. Parameter Estimation and Calibration/`](1.%20Parameter%20Estimation%20and%20Calibration) | MinIO and NFS micro-benchmarks, system parameters, calibrated platform parameters `κ` |
| [`2. Cost-Time Model Accuracy/`](2.%20Cost-Time%20Model%20Accuracy) | Estimated vs. measured `T` and `C` (19 queries × 3 systems × 15 runs) |
| [`3. Cost-Time Plan Optimization/`](3.%20Cost-Time%20Plan%20Optimization) | Pareto fronts, selected plans, BF sensitivity, generated pipelines (`default`, `knee`, `min_T`, `min_C`, `front_*`) |
| [`4. End-to-End Evaluation/`](4.%20End-to-End%20Evaluation) | Measured runs of the optimized plans compared with the baselines |

## Evaluation

Systems compared: **Starling-based** (no BF), **BLOOM-FaaS S3 / NFS** (every BF on), and **CoFi-FaaS** plans
(knee, min-time, min-money) chosen by the optimizer.

### 1. Parameter estimation and calibration — [`tuning-parameters.ipynb`](1.%20Parameter%20Estimation%20and%20Calibration/tuning-parameters.ipynb)

| Storage | Single function | Peak aggregate | Fit error (Aggregate/per function) |
|---|---|---|------------------------------------|
| MinIO | 234 MB/s | 2.10 GB/s (`Bmax` 2403 MB/s) | 4% / 9%                            |
| NFS | 74 MB/s | 0.76 GB/s (`Bmax` 781 MB/s) | 6% / 8%                            |

### 2. Cost–time model accuracy

Mean absolute percentage error (MAPE) over 19 queries × 3 seeds × 3 runs, leave-one-query-out calibration:

| System | MAPE `T` | MAPE `C` |
|---|---|---|
| No BF (Starling-based) | 18.0% | 12.5% |
| BLOOM-FaaS S3-BF | 16.5% | 13.4% |
| BLOOM-FaaS FS-BF | 18.5% | 20.4% |
| **All systems** | **17.7%** | **15.4%** |

### 3. Cost–time plan optimization

The optimizer explores 79–1,604 plans per query and keeps the Pareto front in (`T`, `C`). Estimated totals over the
19 queries:

| Plan | `T` (s) | `C` (¢) | ΔT vs default | ΔC vs default |
|---|---|---|---|---|
| No BF | 442.2 | 137.8 | +56% | +167% |
| Default (every BF on, 50 MB) | 283.1 | 51.6 | — | — |
| **Knee** | **236.2** | **44.8** | **−17%** | **−13%** |

The number of functions per stage is simulated exactly for 276/290 stages. Validation on the testbed: 145 distinct
plans, 447 runs; estimated MAPE 18.5% (`T`) and 18.7% (`C`); the estimated cost order of plan pairs is kept in 89.5%
of the pairs.

### 4. End-to-end evaluation

Measured totals over the 19 queries (median of the runs):

| System | `T` (s) | `C` (¢) | ΔT / ΔC vs BF always on | ΔT / ΔC vs Starling-based |
|---|---|---|---|---|
| Starling-based (no BF) | 550.1 | 160.5 | +79.2% / +198.1% | — |
| BLOOM-FaaS (BF always on) | 306.9 | 53.9 | — | −44.2% / −66.5% |
| **CoFi-FaaS knee** | **300.3** | **48.0** | **−2.1% / −10.9%** | **−45.4% / −70.1%** |
| CoFi-FaaS min-time | 311.7 | 62.0 | +1.6% / +15.2% | −43.3% / −61.4% |
| CoFi-FaaS min-money | 369.0 | 42.6 | +20.2% / −20.9% | −32.9% / −73.5% |

Per query, the knee plan is up to 1.32× faster (Q14) and saves up to 35% of the cost relative to the default plan.

## Run

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
```

## Acknowledgments

Experiments presented in this paper were carried out using the Grid'5000 testbed, supported by a scientific interest
group hosted by Inria and including CNRS, RENATER and several Universities as well as other organizations (see
https://www.grid5000.fr).
