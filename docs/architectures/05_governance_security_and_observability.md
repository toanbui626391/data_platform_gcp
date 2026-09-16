# Solution Architecture: Unified Kubernetes AI & Data Platform on Google Cloud (GCP)
## Document 05: Governance, Security, Privacy & Full-Stack Observability on GCP

---

## 1. Regulatory Compliance & Inline Privacy Guardrails: Vietnam Decree 13 PDPD

The platform strictly complies with the **Vietnam Personal Data Protection Decree (Decree 13/2023/ND-CP)** across batch, streaming, and AI workloads:

1. **Inline Streaming Anonymization (Flink & Debezium):**
   * Before raw transactional data is broadcast across Kafka or committed to Iceberg, Flink streaming operators execute inline cryptographic hashing and tokenization on PII fields (e.g., Vietnamese National ID / CCCD, phone numbers, banking account numbers).
   * Decryption keys are stored in **Google Cloud KMS (CMEK)** and restricted via IAM condition policies.
2. **AI Agent Privacy & Guardrails Proxy:**
   * An inline lightweight proxy intercepts all agent prompt payloads.
   * Named Entity Recognition (NER) detects and redacts customer PII before payloads leave the internal VPC perimeter for external LLM inference.
3. **Data Subject Rights (Article 9 Right to Erasure / RTBF):**
   * Iceberg tables support metadata-driven equality/position deletes, enabling targeted deletion of customer records across petabyte-scale historical datasets without rewriting entire partition directories.

```mermaid
flowchart LR
    subgraph DataSources["Data Sources & Ingestion"]
        App["App Event / CDC"]:::gray
    end

    subgraph StreamingSecurity["Streaming & Ingress Privacy Filter"]
        FlinkMask["Flink Inline Stream Masker\n(Hashes CCCD, Mask PII)"]:::danger
        Proxy["AI Privacy Guardrails Proxy\n(Regex / NER Tokenizer)"]:::danger
        KMS["Cloud KMS (CMEK)\nEnvelope Encryption"]:::gray
    end

    subgraph SecuredSinks["Secured Storage & LLM"]
        KafkaSecure["Kafka (PII-Cleaned)"]:::warning
        IcebergSecure["Iceberg on GCS (Masked)"]:::success
        vLLMSecure["vLLM Engine (Sanitized Prompts)"]:::purple
    end

    App --> FlinkMask --> KafkaSecure --> IcebergSecure
    FlinkMask <--> KMS
    App --> Proxy --> vLLMSecure
    Proxy <--> KMS

    %% Subgraph Styling (Transparent & Dual-Mode Friendly)
    style DataSources fill:none,stroke:#475569,stroke-width:1.5px,stroke-dasharray: 5 5,color:#94a3b8
    style StreamingSecurity fill:none,stroke:#f43f5e,stroke-width:1.5px,stroke-dasharray: 4 4,color:#fb7185
    style SecuredSinks fill:none,stroke:#10b981,stroke-width:1.5px,stroke-dasharray: 4 4,color:#6ee7b7

    %% Link Styling
    linkStyle default stroke:#64748b,stroke-width:2px;

    %% Universal Dual-Mode High-Contrast Palette
    classDef primary fill:#1e3a8a,stroke:#3b82f6,stroke-width:2px,color:#ffffff;
    classDef success fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#ffffff;
    classDef warning fill:#451a03,stroke:#f59e0b,stroke-width:2px,color:#ffffff;
    classDef danger  fill:#4c0519,stroke:#f43f5e,stroke-width:2px,color:#ffffff;
    classDef purple  fill:#2e1065,stroke:#a855f7,stroke-width:2px,color:#ffffff;
    classDef gray    fill:#0f172a,stroke:#475569,stroke-width:1.5px,color:#e2e8f0;
```

---

## 2. Automated Metadata, Lineage & Cataloging: OpenMetadata on GKE

**OpenMetadata** runs natively on GKE with a PostgreSQL backend (CloudNativePG) and OpenSearch:
* **Dual-Speed Lineage Tracking:** Captures both the **Sub-second Speed Layer** (`Kafka -> Flink -> Redis/pgvector -> Agent`) and the **Analytical Lakehouse Layer** (`Kafka -> Flink -> Iceberg Bronze -> Spark -> Iceberg Gold -> Trino/BI`).
* **Lakekeeper & Iceberg Integration:** Automatically ingests table schemas, column data types, partition specs, and snapshot commit histories.
* **Agent Reasoning Lineage:** Tracks which Iceberg snapshot and vector chunk was retrieved by an agent for any given LLM decision.

```mermaid
flowchart TD
    subgraph Ingestion["Ingestion & Streaming on GKE"]
        Kafka["Kafka on Hyperdisk Balanced"]:::warning
        Flink["Apache Flink Stream Processor"]:::primary
        Kafka --> Flink
    end

    subgraph SpeedLayer["Speed Layer Lineage (< 500ms)"]
        Redis["Redis L1 Feature Store"]:::warning
        pgvector["PostgreSQL pgvector L2"]:::warning
        Agent["Credit Scoring AI Agent"]:::purple
        
        Flink -->|"Sub-5ms"| Redis --> Agent
        Flink -->|"Sub-1s"| pgvector --> Agent
    end

    subgraph LakehouseLayer["Lakehouse Layer Lineage (NRT & Batch)"]
        IcebergBronze["Iceberg Bronze (gs://.../bronze/)"]:::success
        Spark["Spark Batch Aggregation"]:::primary
        IcebergGold["Iceberg Gold (gs://.../gold/)"]:::success
        BI["Executive PowerBI Dashboard"]:::primary
        
        Flink -->|"60s Micro-batch"| IcebergBronze --> Spark --> IcebergGold --> BI
    end

    subgraph Catalog["OpenMetadata Central Governance Graph"]
        Graph["Visual End-to-End Lineage, PII Tags & SLA Health"]:::cyan
    end

    Redis -.-> Graph
    pgvector -.-> Graph
    Agent -.-> Graph
    IcebergBronze -.-> Graph
    IcebergGold -.-> Graph
    BI -.-> Graph

    %% Subgraph Styling (Transparent & Dual-Mode Friendly)
    style Ingestion fill:none,stroke:#f59e0b,stroke-width:1.5px,stroke-dasharray: 4 4,color:#fcd34d
    style SpeedLayer fill:none,stroke:#a855f7,stroke-width:1.5px,stroke-dasharray: 4 4,color:#d8b4fe
    style LakehouseLayer fill:none,stroke:#10b981,stroke-width:1.5px,stroke-dasharray: 4 4,color:#6ee7b7
    style Catalog fill:none,stroke:#06b6d4,stroke-width:1.5px,stroke-dasharray: 4 4,color:#67e8f9

    %% Link Styling
    linkStyle default stroke:#64748b,stroke-width:2px;

    %% Universal Dual-Mode High-Contrast Palette
    classDef primary fill:#1e3a8a,stroke:#3b82f6,stroke-width:2px,color:#ffffff;
    classDef success fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#ffffff;
    classDef warning fill:#451a03,stroke:#f59e0b,stroke-width:2px,color:#ffffff;
    classDef purple  fill:#2e1065,stroke:#a855f7,stroke-width:2px,color:#ffffff;
    classDef cyan    fill:#083344,stroke:#06b6d4,stroke-width:2px,color:#ffffff;
```

---

## 3. GCP Security Perimeter: VPC Service Controls & IAM

```mermaid
flowchart TB
    subgraph VPCSCPerimeter["Google Cloud VPC Service Controls Perimeter"]
        subgraph GKEVPC["GKE Private Cluster VPC"]
            GKENodes["GKE Worker Nodes\n(No Public IPs)"]:::primary
            PrivatePSC["Private Service Connect\nEndpoints"]:::cyan
        end

        subgraph GCPStorage["Secured GCP APIs"]
            GCS["Google Cloud Storage\n(CMEK Encrypted)"]:::success
            KMS["Cloud KMS Key Rings"]:::gray
            Logging["Cloud Logging & Monitoring"]:::gray
        end

        GKENodes <--> PrivatePSC <--> GCPStorage
    end

    Internet["Public Internet"]:::danger -.->|BLOCKED by VPC-SC| GCPStorage
    Internet -.->|Only via Cloud Armor WAF| GKENodes

    %% Subgraph Styling (Transparent & Dual-Mode Friendly)
    style VPCSCPerimeter fill:none,stroke:#f43f5e,stroke-width:2px,stroke-dasharray: 6 3,color:#fb7185
    style GKEVPC fill:none,stroke:#0ea5e9,stroke-width:1.5px,stroke-dasharray: 4 4,color:#38bdf8
    style GCPStorage fill:none,stroke:#10b981,stroke-width:1.5px,stroke-dasharray: 4 4,color:#6ee7b7

    %% Link Styling
    linkStyle default stroke:#64748b,stroke-width:2px;

    %% Universal Dual-Mode High-Contrast Palette
    classDef primary fill:#1e3a8a,stroke:#3b82f6,stroke-width:2px,color:#ffffff;
    classDef success fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#ffffff;
    classDef danger  fill:#4c0519,stroke:#f43f5e,stroke-width:2px,color:#ffffff;
    classDef cyan    fill:#083344,stroke:#06b6d4,stroke-width:2px,color:#ffffff;
    classDef gray    fill:#0f172a,stroke:#475569,stroke-width:1.5px,color:#e2e8f0;
```

* **Private GKE Cluster:** Worker nodes have private RFC 1918 IP addresses with zero public IP exposure. All internet egress is mediated via Cloud NAT.
* **Cloud Armor & WAF:** Protects the external Agent API gateway against DDoS attacks, SQL injection, and malicious payloads.

---

## 4. Full-Stack Observability Subsystem on GCP

Observability unifies infrastructure telemetry, stream processing health, and AI agent reasoning:

```mermaid
flowchart TB
    subgraph TelemetrySources["Telemetry Sources on GKE"]
        K8sNodes["GKE Nodes & CNI\n(cAdvisor, metrics)"]:::gray
        KafkaMetrics["Kafka Brokers\n(Lag & IOPS Exporter)"]:::warning
        FlinkMetrics["Flink TaskManagers\n(Checkpoints & Backpressure)"]:::primary
        Engines["Trino / Spark\n(JMX Exporters)"]:::primary
        LLM["vLLM Engine\n(Prometheus Metrics)"]:::purple
        AgentSpans["AI Agent Reasoning\n(OpenTelemetry GenAI)"]:::cyan
    end

    subgraph CollectionLayer["Telemetry Collection on GCP"]
        GMP["Google Cloud Managed\nPrometheus (GMP)"]:::cyan
        OTelCol["OpenTelemetry Collector\non GKE"]:::cyan
        CloudLog["Google Cloud Logging\n(Central Ingestion)"]:::gray
    end

    subgraph VisualizationLayer["Monitoring & Incident Dashboards"]
        Grafana["Grafana HA Dashboard\n(Streaming, DB & AI Observability)"]:::primary
        CloudMon["Google Cloud Monitoring\n& Real-Time Alerting"]:::success
        AgentEval["Langfuse / MLFlow\n(Agent Quality & Token FinOps)"]:::purple
    end

    TelemetrySources --> GMP
    TelemetrySources --> OTelCol
    TelemetrySources --> CloudLog
    GMP --> Grafana
    GMP --> CloudMon
    OTelCol --> AgentEval

    %% Subgraph Styling (Transparent & Dual-Mode Friendly)
    style TelemetrySources fill:none,stroke:#475569,stroke-width:1.5px,stroke-dasharray: 5 5,color:#94a3b8
    style CollectionLayer fill:none,stroke:#06b6d4,stroke-width:1.5px,stroke-dasharray: 4 4,color:#67e8f9
    style VisualizationLayer fill:none,stroke:#3b82f6,stroke-width:1.5px,stroke-dasharray: 4 4,color:#93c5fd

    %% Link Styling
    linkStyle default stroke:#64748b,stroke-width:2px;

    %% Universal Dual-Mode High-Contrast Palette
    classDef primary fill:#1e3a8a,stroke:#3b82f6,stroke-width:2px,color:#ffffff;
    classDef success fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#ffffff;
    classDef warning fill:#451a03,stroke:#f59e0b,stroke-width:2px,color:#ffffff;
    classDef purple  fill:#2e1065,stroke:#a855f7,stroke-width:2px,color:#ffffff;
    classDef cyan    fill:#083344,stroke:#06b6d4,stroke-width:2px,color:#ffffff;
    classDef gray    fill:#0f172a,stroke:#475569,stroke-width:1.5px,color:#e2e8f0;
```

### Key Service Level Indicators (SLIs) & Alert Matrix

| Component | Metric Name | Target SLA | Alert Condition |
| :--- | :--- | :--- | :--- |
| **Kafka Cluster** | Consumer Group Lag | Lag $< 5,000$ messages | `kafka_consumergroup_lag > 10000` for 3m |
| **Kafka Brokers** | P99 Produce Latency | Latency $< 5\text{ ms}$ | `kafka_server_requestmetrics_requestspersec{request="Produce"} > 20ms` |
| **Flink Streaming** | Checkpoint Duration | Duration $< 20\text{ seconds}$ | `flink_jobmanager_job_lastCheckpointDuration > 30000` for 2 checkpoints |
| **Flink Streaming** | Stream Backpressure Ratio | Ratio $< 0.15$ | `flink_taskmanager_job_task_isBackPressured > 0.30` for 5m |
| **vLLM Serving** | Time-To-First-Token (TTFT) | P95 $< 250\text{ ms}$ | `vllm:avg_prompt_throughput_tok_per_s < 100` for 5m |
| **vLLM Serving** | GPU VRAM KV-Cache Pressure | Utilization $< 90\%$ | `vllm:gpu_cache_usage_factor > 0.90` for 3m |
| **Trino Engine** | Queued Analytical Queries | P95 $< 2.0\text{s}$ | `trino_queued_queries > 20` for 5m (Triggers KEDA scale-out) |
| **GCS Storage** | 4xx / 5xx API Error Rate | Error Rate $< 0.01\%$ | `gcs_api_error_rate > 0.01` for 5m |
| **AI Agents** | Tool Execution Failure Rate | Failure Rate $< 2\%$ | `agent_tool_error_count / total > 0.05` for 5m |
