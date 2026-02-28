# OpenFEMA - Runbook (Lean)

This runbook is intentionally minimal: global decisions + milestone checklists.
It will grow as the project progresses.

---

## Global decisions (Frozen)

### Environments
- DEV / STG / PROD
- STG/PROD are **shell workspaces** (no compute unless explicitly enabled later)

### Stack (strict)
- ADLS Gen2 (landing/raw + ops + quarantine + logs + UC Metastore)
- dlthub (API ingestion → landing files)
- Azure Databricks (Bronze/Silver/Gold Delta + Jobs/Workflows)
- Python/PySpark for Landing to Silver and SQL/SparkSQL for Gold

### Operational rules
- Every run has a `load_id` (UUIDv4)
- Raw landing is immutable: never overwrite; re-ingestion => new `load_id`
- No real PROD data is moved to DEV (only sanitized repro packs if needed)

---

# M0 — Foundation (Completed)
- [x] Ensure all Databricks Workspaces permissions to read and write in the Azure Storage Account
- [x] Repo initialized with README
- [x] Minimal repo security rules in place (protect main via PR + checks)
- [x] Databricks Bundles backbone created (`databricks.yml` + env targets)
- [x] GitHub Actions:
  - [x] CI: lint/format checks on PRs
  - [x] CD: deploy DEV on main; manual deploy STG/PROD
- [x] Databricks governance baseline established (Unity Catalog / metastore + env catalogs)
- [x] ADLS baseline created (landing/ops/quarantine/logs containers)

---

# M1 — Ingestion (Planned)
Goal: ingest OpenFEMA entities into ADLS landing as raw files (via dlthub) with run correlation.

- [x] Define OpenFEMA sources + entities (initial set)
- [ ] Implement dlthub pipeline for at least 1 entity
- [ ] Write raw files to ADLS landing
- [ ] Write a per-run manifest in ops (files written + row counts)
- [ ] Ensure idempotency (no overwrite; each run = new `load_id`)