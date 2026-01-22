# SleepLens Snowflake Integration Walkthrough

This document outlines the successful integration of Snowflake into the SleepLens pipeline. We have implemented an end-to-end flow that ingests generated sleep data, loads it into Snowflake, transforms it using dbt, and visualizes the results in a Streamlit dashboard.


## Project Status
*   **Repository**: `https://github.com/kahramanmurat/sleeplens`
*  - **CI/CD**: ✅ Active & Passing (GitHub Actions)
- **Repo Hygiene**: ✅ Clean (No secrets/data committed)
- **Deployment**: ✅ Docker-ready with automated builds (GitHub Actions)
*   **Documentation**: Comprehensive README and Architecture Map included.

## System Architecture


1.  **Ingestion**: `fetch_public_summary.py` uses **PySpark** to generate synthetic sleep data (bronze/raw) at scale.
2.  **Load**: `upload_to_snowflake.py` loads raw CSV data into Snowflake `SLEEPLENS.RAW` schema tables (`SLEEP_STUDIES`, `EVENTS`).
3.  **Transformation**: dbt models transform raw data into:
    *   **Staging**: `STG_SLEEP_STUDY`, `STG_EVENTS` (normalized and cleaned).
    *   **Marts**: `FCT_SLEEP_SUMMARY`, `FCT_EVENT_RATES`, `DIM_TIME` (business-ready facts and dimensions).
4.  **Scientific Integration (PhysioNet)**:
    *   **Ingestion**: `fetch_physionet.py` downloads real PSG recordings (EDF) using **MNE-Python**.
    *   **Feature Extraction**: Processes raw EEG signals to compute Spectral Power Density (Delta, Theta, Alpha, Beta, Gamma) per 30s epoch.
    *   **Analytics**: dbt models (`stg_signal_features`, `fct_signal_analytics`) aggregate these bio-signals.
5.  **Visualization**: Streamlit dashboard queries dbt marts in Snowflake to display insights, including Hypnograms and Spectral Trends.

## Prerequisites

*   Docker & Docker Compose
*   Snowflake Account (Credential in `transformations/dbt/profiles.yml`)

## Running the Pipeline

The easiest way to run the full pipeline is using the orchestration script:

```bash
./scripts/run_snowflake.sh
```

This script performs the following steps:
1.  Builds the Docker images.
2.  Generates synthetic data for the current date.
3.  Uploads the data to Snowflake.
4.  Runs dbt transformations (run & test).

The dashboard has been updated to provide clinical insights:
    *   **Sidebar Stats**: Global dataset metrics (Total Patients, Date Range).
    *   **Overview**: Key metrics including average AHI and severe apnea percentage.
    *   **Population Health**: AHI distribution, age correlations, and sleep duration analysis.
    *   **Patient Details**: Drill-down view for individual patients with severity scoring and peer comparisons.

To run the dashboard:

```bash
uv run streamlit run dashboard/app.py
```

## Verification Results

### Infrastructure
*   **Terraform**: The `infra/terraform` configuration is valid and matches the intended state (S3 buckets, IAM roles). Verified via `terraform plan`.

### Orchestration
*   **Kestra**: A Kestra flow definition `orchestration/kestra/flow_full_pipeline.yaml` has been created to orchestrate this process in production.
    *   **Pipeline**: `sleeplens_pipeline` (Daily scheduled runs)
    *   **Backfill**: `sleeplens_backfill` (Historical data generation)
    *   **Configuration**:
        *   **Persistence**: PostgreSQL (prevents flow loss on restart).
        *   **Security**: `volume-enabled: true` (required for file mounting).
        *   **Runner**: Docker-in-Docker with explicit volume mounts.
    *   *Note*: To bypass local authentication complexities, the `docker-compose.yml` is configured to use Kestra `v0.19.1`. The flow definition has been adapted for this version (sequential execution by default, no `dependsOn`).
    *   **Schedule**: The pipeline is configured to run daily at **09:00 UTC**.

## Artifacts Created
*   `ingestion/load/upload_to_snowflake.py`: Dedicated Snowflake loader.
*   `transformations/dbt/`: Full dbt project with staging and mart models.
*   `dashboard/app.py`: Updated Streamlit app with Snowflake connector.
*   `scripts/run_snowflake.sh`: Shell-based orchestrator.
