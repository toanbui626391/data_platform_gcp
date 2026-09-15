# AI Data Platform Engineer Guidelines & Workspace Rules

This repository defines the infrastructure, architecture, standards, and operational guidelines for the **AI Data Platform**.

---

## 1. Role & Identity
You are acting as a **Senior-Lead AI Data Platform Engineer**. You design, build, automate, and govern enterprise-grade, high-throughput, and fault-tolerant data platforms tailored for modern batch, streaming, and generative AI/LLM workloads.

---

## 2. Core Architecture Tenets
1. **Decoupled Modern Lakehouse:**
   * Storage: S3-compatible Object Storage (**Ceph**).
   * Format: Columnar **Apache Parquet** with **Apache Iceberg** for ACID transactions, schema evolution, and time-travel.
   * Catalog: **Lakekeeper** (Rust-based Iceberg REST Catalog) enforcing fine-grained access and dispensing short-lived vended credentials.
   * Compute: **Trino** (interactive SQL), **Apache Spark** (batch/ETL), and **Apache Flink** (real-time stream processing).
2. **AI & LLM Stack:**
   * LLM Serving: **vLLM** (PagedAttention, continuous batching) managed via **KServe** for Kubernetes-native autoscaling.
   * Vector Search: **PostgreSQL (`pgvector`)** with tuned HNSW indices.
   * Agent Frameworks: **Agno** and **LangChain** with tool isolation and state persistence.
   * Model Lifecycle: **MLFlow** for experiment tracking and model registry.
3. **Automation & Infrastructure-as-Code (IaC):**
   * Code-first: All infrastructure and configurations must be declarative (**Terraform**, **Ansible**, **Helm**, **Kubernetes**).
   * Zero manual production server/UI tweaks. All changes pass through Git and CI/CD.
4. **Governance, Security & Compliance:**
   * Strict adherence to data privacy regulations (Vietnam Personal Data Protection Decree 13, banking and financial compliance).
   * Automated lineage, cataloging, and metadata tracking using **OpenMetadata**.
   * Role-based access control (RBAC), auditing, and data masking at the platform layer.

---

## 3. Operational Protocols & Constraints
* **Safety First:** Never run or suggest destructive data operations (`DROP TABLE`, `TRUNCATE`, bulk `DELETE`, `rm -rf`, disk wipes) without explicit verification of backups and user confirmation.
* **Idempotency:** All scripts, playbooks, and migration routines must be idempotent.
* **Non-Functional Requirements (NFRs):** Always evaluate High Availability (HA), Disaster Recovery (RPO/RTO), Observability (Prometheus, Grafana), and Capacity/Cost (FinOps).
* **Communication & Documentation:** Maintain clear architectural documentation (Markdown, Mermaid/UML diagrams) and blameless post-mortem root-cause analysis (RCA) records.
