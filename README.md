# Podcast Listening Analysis – **Headphone Heroes** 🎧

> **Dataset & Evaluation**: Kaggle *Playground Series S5 E4* – predict minutes listened, scored by **RMSE**. (<https://www.kaggle.com/competitions/playground-series-s5e4>)

A single Jupyter notebook is fine for exploration—but brittle for hand‑offs, hyper‑parameter sweeps, and repeatable night‑runs. This repo shows how to turn the very same Kaggle workflow into a **production‑grade, containerised data pipeline** without renting a single cloud VM.

---

## ⚙️ Tech stack & _why it beats the alternatives_

| Layer | Chosen tech | Why not something else? |
|-------|-------------|-------------------------|
| **Language** | **Python 3.11** | Ubiquitous in DS; rich ML ecosystem; still fast enough once the heavy lifting is in C++ libs. |
| **Distributed compute / ETL** | **Apache Spark 3.5 + PySpark** | Handles 10⁶–10⁷ rows on a laptop and 10⁹ on a cluster with the same code. Pandas or Polars choke at scale or require explicit sharding. |
| **Model** | **XGBoost‑Spark 1.7** | State‑of‑the‑art for tabular data; supports both CPU & GPU; integrates with Spark’s DataFrame API → zero glue code. LightGBM needs a separate cluster or ugly Pandas fall‑back. |
| **Orchestration** | **Apache Airflow 2.9** | Mature DAG scheduler, rich UI, sensors, retries. Cron scripts break on failure; Prefect/Covalent fresh but less battle‑tested. |
| **Containerisation** | **Docker + docker‑compose** | Guarantees “runs on my machine” parity, even on CI; compose spins up all services with a single command. Kubernetes is overkill for two containers. |
| **Storage** | **Parquet + Arrow IPC** | Columnar, compressed, seeks only needed columns; Arrow keeps zero‑copy interoperability with pandas for quick plots. CSV is 6× slower & fatter. |
| **Hyper‑parameter tuning** | **Optuna** (optional) | Asynchronous TPE sampler works out‑of‑the‑box with Spark’s driver program; no extra service required. |
| **Lint / CI** | **pre‑commit + Ruff + Black** | Keeps PRs clean and build‑ready—fast (<0.1 s) toolchain. |

---

## 🗂️ Repo layout (TL;DR)
```
Podcast-Listening-Analysis/
├── airflow/          # Custom Airflow image & DAGs
│   ├── dags/
│   │   └── pipeline_dag.py   # Schedules the Spark job
│   └── Dockerfile
│
├── spark-app/        # All heavy lifting lives here
│   ├── pipeline.py   # ETL → FE → train → predict → export
│   └── Dockerfile
│
├── data/             # Drop Kaggle CSVs here; outputs land here too
├── docker-compose.yml
└── README.md
```

---

## 🔄 How the pipeline flows
1. **Airflow DAG** fires daily (or on‑push).
2. **Spark job** inside `spark-app` container reads `/data/train.csv` & `/data/test.csv`.
3. **Feature engineering**
   * `is_weekend`, `pub_hour`, `host_guest_ratio`, `ads_per_minute`, `sentiment_score`…
4. **Model training**: Spark‑XGBoost – depth 8, lr 0.05, 2 000 rounds, early‑stopping on 20 % validation split.
5. **Prediction** on test → Parquet → CSV.
6. **(Optional)** last task uploads via Kaggle CLI if an API token is present.

Everything is **stateless**. Kill the containers → spin again → identical artefacts.

---

## 🧱 Extending the baseline
* **Text embeddings** – drop `Episode_Description` into `spark-nlp` → Universal Sentence Encoder.
* **Ensembles** – schedule multiple models (LightGBM, CatBoost) and blend.
* **Auto‑tune** – wrap Optuna study around hyper‑params; Airflow can parallelise trials across task instances.
* **Monitoring** – add MLflow tracking server container; log metrics & artefacts automatically.

PRs welcome – follow **Conventional Commits** and run `pre‑commit run -a` before pushing.

---

## 📜 License
This project is licensed under the [MIT License](LICENSE). Feel free to use, modify, and distribute it as you see fit.

> Data © Kaggle (synthetic). Code © the contributors.