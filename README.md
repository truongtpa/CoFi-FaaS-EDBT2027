# CoFi-FaaS: A Cost Model for Bloom Filter Pushdown in Serverless Joins

Data, notebooks and figures of the paper (EDBT 2027): a cost-time model for serverless query plans with
Bloom-filter pushdown. TPC-H SF100 on Grid'5000.

- **`BLOOM-FaaS Aug 21 Full (Technical report).pdf`**: the base system architecture used to run the code.
- **`Cofi/`**: code of the cost-time model, which computes the processing time `T` and the monetary cost `C`
  (`model.py`) and the plan optimizer (`plans.py`).
- **`1. Parameter Estimation and Calibration/`**: micro-benchmarks of MinIO and NFS, and the system parameters
  and platform parameters `κ` used by the model.
- **`2. Cost-Time Model Accuracy/`**: estimated vs. measured time and cost (19 queries × 3 systems × 15 runs).
- **`3. Cost-Time Plan Optimization/`**: the optimizer's Pareto fronts and selected plans, and the generated
  pipelines.
- **`4. End-to-End Evaluation/`**: measured runs of the optimized plans compared with the baselines.

Each section has a notebook, `data/` (input data) and `figures/` (output figures).

## Run

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt
```

Open a notebook in Jupyter and run all cells.
