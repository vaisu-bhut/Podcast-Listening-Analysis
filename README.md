# 🎧 Podcast-Listening Analysis

Predict **how long listeners will stay with a podcast episode** and surface the drivers behind great retention — at scale, on a laptop, with zero cloud bills.

<p align="center">
  <img src="https://img.shields.io/badge/Spark-3.3-blue"/>
  <img src="https://img.shields.io/badge/Airflow-2.7-blue"/>
  <img src="https://img.shields.io/badge/XGBoost-1.7-blue"/>
  <img src="https://img.shields.io/badge/Docker-v3.7-blue"/>
</p>

---

## 1 Why this repo exists
Jupyter notebooks are perfect for exploration but painful for nightly retrains, team hand-offs and CI/CD.  
This project shows how to **lift a notebook-level experiment into a production-grade, fully containerised data-science pipeline** that anyone can run with one command.

---

## 2 Solution architecture

| Layer | Purpose | Technology |
|-------|---------|------------|
| **Orchestration** | Schedule, retry and monitor the end-to-end DAG | Apache Airflow 2.7 (`DockerOperator`) |
| **Compute / ETL** | Read CSV → feature-engineer → train XGBoost → generate predictions & viz dataset | Spark 3.3 + PySpark, Spark-XGBoost 1.7 |
| **Container runtime** | Reproducible, one-command spin-up | Docker & docker-compose v3.7 |
| **Storage** | Raw data & model artefacts | Local `data/` volume (Parquet + CSV) |

Everything is stateless; kill the containers and spin them up again → identical artefacts.

---

## 3 Tech-stack decisions (TL;DR)

| Choice | Why it beats the usual alternatives |
|--------|-------------------------------------|
| **PySpark** | Same code scales from millions of rows on a laptop to billions on a cluster. |
| **Spark-XGBoost** | State-of-the-art for tabular regression; native DataFrame API eliminates glue code. |
| **Airflow** | Mature UI, back-fills, sensors and robust retry semantics. |
| **Docker-Compose over Kubernetes** | Two containers don’t justify a full K8s control plane. |

---

## 4 Repo layout

```text
Podcast-Listening-Analysis/
├── airflow/            # Custom Airflow image & DAGs
│   ├── dags/podcast_dag.py
│   └── Dockerfile
├── spark-app/          # Spark job image
│   ├── podcast_pipeline.py
│   └── Dockerfile
├── data/               # Place raw CSVs here; outputs land here too
├── docker-compose.yml
└── README.md
```

## 5 Inside the Spark job

| Stage | Key logic |
|-------|-----------|
| **Feature engineering** | Weekend flag, publication hour, host-to-guest popularity ratio, ad density, sentiment score… |
| **ML pipeline** | `Imputer` → `StringIndexer` (4 categoricals) → `VectorAssembler` → `SparkXGBRegressor` |
| **Model hyper-params** | `max_depth` = 8, `eta` = 0.05, 100 trees (early-stop on validation RMSE) |

See `spark-app/podcast_pipeline.py` for full details.

---

## 6 Customisation tips

* **Add new features** – edit `feature_engineering()` and rebuild the `spark-app` image.  
* **Hyper-parameter sweeps** – wrap the training stage in an Optuna study; Airflow can fan-out trials across parallel task instances.  
* **Experiment tracking** – drop an MLflow server container into `docker-compose.yml` and log metrics & artefacts automatically.  
* **Alternate scheduler** – swap Airflow for Prefect by replacing the `airflow/` directory and the compose service.

---

## 7 Troubleshooting

| Symptom | Likely cause & quick fix |
|---------|-------------------------|
| **Airflow task stuck in `queued`** | Docker Engine socket not mounted; ensure `/var/run/docker.sock` is listed under `volumes:` for the `airflow` service. |
| **Spark job OOMs** | Bump `spark.driver.memory` / `spark.executor.memory` in `podcast_pipeline.py`. |
| **Data not found in container** | The DAG mounts your *host* `data/` folder into `/data` **read-only**. Update `HOST_DATA` in `airflow/dags/podcast_dag.py` if you move the repo. |

---

## 8 Contributing

PRs welcome!  
* Follow **Conventional Commits**.  
* Run `pre-commit run -a` before pushing.

---

## 9 License

MIT — see [`LICENSE`](./LICENSE).  
*Code © contributors. Data ownership remains with the original provider.*

