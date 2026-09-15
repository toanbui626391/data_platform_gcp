# Dual-Mode System Architecture Diagram Example

This example illustrates an end-to-end production architecture diagram that is crystal clear, vibrant, and effortlessly legible in both Light and Dark modes.

---

## 1. Architectural Diagram

```mermaid
flowchart TB
    subgraph ClientZone["Client & Consumer Tier"]
        ClientApp["Mobile / Web App"]:::gray
        BIUser["BI Analyst (PowerBI)"]:::gray
    end

    subgraph SecurityPerimeter["VPC Security & Governance Perimeter"]
        Gateway["Cloud Armor & Envoy Gateway"]:::danger
        PIIProxy["Decree 13 PII Masking Proxy"]:::danger
    end

    subgraph AgenticCore["AI Agent & LLM Serving Subsystem"]
        AgnoWorker["Agno Agent Worker Pods"]:::purple
        vLLMEngine["vLLM Engine (NVIDIA L4)"]:::purple
        Sandbox["gVisor Tool Execution Sandbox"]:::danger
    end

    subgraph LakehouseSubstrate["Open Lakehouse & Storage Fabric"]
        Trino["Trino Distributed SQL Engine"]:::primary
        Lakekeeper["Lakekeeper REST Catalog"]:::cyan
        GCS["Google Cloud Storage (Iceberg)"]:::success
        VectorDB["PostgreSQL pgvector Store"]:::warning
    end

    %% Interaction Paths
    ClientApp --> Gateway --> PIIProxy --> AgnoWorker
    BIUser --> Gateway --> Trino

    AgnoWorker <-->|"v2 API"| vLLMEngine
    AgnoWorker -->|"Execute Code"| Sandbox
    AgnoWorker <-->|"Semantic Context"| VectorDB
    Sandbox <-->|"Run SQL"| Trino

    Trino <-->|"REST Metadata"| Lakekeeper
    Trino <-->|"Read/Write Parquet"| GCS
    Lakekeeper <-->|"Tracks Snapshots"| GCS

    %% Subgraph Styling (Transparent & Dual-Mode Friendly)
    style ClientZone fill:none,stroke:#64748b,stroke-width:1.5px,stroke-dasharray: 5 5,color:#94a3b8
    style SecurityPerimeter fill:none,stroke:#f43f5e,stroke-width:2px,stroke-dasharray: 6 3,color:#fb7185
    style AgenticCore fill:none,stroke:#a855f7,stroke-width:1.5px,stroke-dasharray: 5 5,color:#c084fc
    style LakehouseSubstrate fill:none,stroke:#0ea5e9,stroke-width:1.5px,stroke-dasharray: 5 5,color:#38bdf8

    %% Universal Dual-Mode High-Contrast Palette
    classDef primary fill:#1e3a8a,stroke:#3b82f6,stroke-width:2px,color:#ffffff;
    classDef success fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#ffffff;
    classDef warning fill:#451a03,stroke:#f59e0b,stroke-width:2px,color:#ffffff;
    classDef danger  fill:#4c0519,stroke:#f43f5e,stroke-width:2px,color:#ffffff;
    classDef purple  fill:#2e1065,stroke:#a855f7,stroke-width:2px,color:#ffffff;
    classDef cyan    fill:#083344,stroke:#06b6d4,stroke-width:2px,color:#ffffff;
    classDef gray    fill:#0f172a,stroke:#475569,stroke-width:1.5px,color:#e2e8f0;
```

---

## 2. Why This Renders Flawlessly in Light & Dark Mode:
1. **Zero Text Bleed:** Every node class explicitly locks `color:#ffffff` or `#e2e8f0`, guaranteeing that regardless of whether the browser background is `#FFFFFF` or `#0D1117`, text is sharp and legible.
2. **Distinctive Card Depth:** The deep jewel backgrounds (`#1e3a8a`, `#064e3b`, `#2e1065`) create crisp, modern cards on white canvases and blend into dark canvases without looking washed out.
3. **Luminous Outlines:** Neon-tinted 2px borders (`#3b82f6`, `#10b981`, `#a855f7`, `#f43f5e`) delineate node perimeters against pitch-black dark mode backgrounds.
4. **Transparent Boundaries:** Subgraphs use `fill:none` and themed dashed borders (`stroke-dasharray: 5 5`), avoiding ugly solid white or solid gray boxes.
