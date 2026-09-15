# Solution Architecture: Unified Kubernetes AI & Data Platform on Google Cloud (GCP)
## Document 03: AI Agent Runtimes, LLM Serving & Memory Architecture on GKE

---

## 1. AI Agent Workload Architecture on GKE

AI Agent workloads on GKE are engineered for **continuous, low-latency reasoning loops (Plan $\rightarrow$ Tool Call $\rightarrow$ Observe $\rightarrow$ Act)**. GKE provides purpose-built cloud-native capabilities—specifically **GKE Sandbox (managed gVisor)**, **GCS FUSE CSI driver**, and **GPU Autoscaling (KEDA)**—that make it an optimal runtime environment for autonomous agents.

```mermaid
flowchart TB
    subgraph ClientAndIngress["Client & Application Layer"]
        User["End Users / Mobile & Web Apps / Internal Tools"]:::gray
    end

    subgraph GKECluster["GKE Regional Cluster (asia-southeast1)"]
        subgraph AgentOrchestration["AI Agent Orchestration Layer"]
            AgentGateway["Agent API Gateway"]:::purple
            AgentWorkers["Agent Worker Pods (Agno / LangChain)\n- Stateless Orchestrator\n- Prompt & Context Assembler"]:::purple
            PrivacyProxy["Data Privacy & Guardrails Proxy\n(Vietnam Decree 13 PDPD Masking)"]:::danger
            
            User --> AgentGateway --> PrivacyProxy --> AgentWorkers
        end

        subgraph LLMServingGKE["LLM Serving Layer (GPU Node Pools)"]
            KServe["KServe Operator & Ingress"]:::purple
            vLLMPods["vLLM Engine Pods (NVIDIA L4 / A100)\n- PagedAttention & Continuous Batching\n- Streaming Model Weights from GCS FUSE"]:::purple
            
            AgentWorkers <-->|"OpenAI / v2 API"| KServe --> vLLMPods
        end

        subgraph SandboxExecution["Untrusted Tool Execution (GKE Sandbox)"]
            Dispatcher["Sandbox Pod Dispatcher"]:::gray
            GKESandboxPods["GKE Sandbox Pods (gVisor runsc)\n- Dynamic Python Code Execution\n- Isolated Ad-hoc Analysis Engine"]:::danger
            
            AgentWorkers <-->|"Execute Tool"| Dispatcher --> GKESandboxPods
        end

        subgraph MCPGateway["Model Context Protocol (MCP) Gateway"]
            MCP["MCP Gateway (Tool & Data Platform Interface)\n- Token-Bucket Rate Limiter\n- RBAC Enforcement"]:::cyan
            
            AgentWorkers <--> MCP
            GKESandboxPods <--> MCP
        end

        subgraph MemoryHierarchyGCP["3-Tier Agent Memory Subsystem"]
            L1["L1: Working Memory\n(Memorystore / Redis Cluster - Sub-ms)"]:::warning
            L2["L2: Semantic Memory\n(PostgreSQL pgvector on Hyperdisk)"]:::warning
            L3["L3: Episodic / Audit Memory\n(Apache Iceberg on GCS Bucket)"]:::success
            
            AgentWorkers <--> L1
            AgentWorkers <--> L2
            AgentWorkers -->|"Async Telemetry"| L3
        end

        subgraph LakehouseAccess["Data Platform Core Access"]
            Trino["Trino (Interactive SQL via MCP)"]:::primary
            Lakekeeper["Lakekeeper REST Catalog"]:::cyan
            
            MCP <--> Trino
            MCP <--> Lakekeeper
            L3 --> Lakekeeper
        end
    end

    subgraph GCPStorageModels["GCP Storage"]
        GCSModels["gs://ai-model-registry/ (GCS Bucket)"]:::success
        GCSModels -.->|"GCS FUSE Mount"| vLLMPods
    end

    %% Subgraph Styling (Transparent & Dual-Mode Friendly)
    style ClientAndIngress fill:none,stroke:#475569,stroke-width:1.5px,stroke-dasharray: 5 5,color:#94a3b8
    style GKECluster fill:none,stroke:#0ea5e9,stroke-width:1.5px,stroke-dasharray: 5 5,color:#38bdf8
    style AgentOrchestration fill:none,stroke:#a855f7,stroke-width:1.5px,stroke-dasharray: 4 4,color:#d8b4fe
    style LLMServingGKE fill:none,stroke:#8b5cf6,stroke-width:1.5px,stroke-dasharray: 4 4,color:#c4b5fd
    style SandboxExecution fill:none,stroke:#f43f5e,stroke-width:1.5px,stroke-dasharray: 4 4,color:#fb7185
    style MCPGateway fill:none,stroke:#06b6d4,stroke-width:1.5px,stroke-dasharray: 4 4,color:#67e8f9
    style MemoryHierarchyGCP fill:none,stroke:#f59e0b,stroke-width:1.5px,stroke-dasharray: 4 4,color:#fcd34d
    style LakehouseAccess fill:none,stroke:#3b82f6,stroke-width:1.5px,stroke-dasharray: 4 4,color:#93c5fd
    style GCPStorageModels fill:none,stroke:#10b981,stroke-width:1.5px,stroke-dasharray: 5 5,color:#6ee7b7

    %% Link Styling
    linkStyle default stroke:#64748b,stroke-width:2px;

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

## 2. High-Throughput LLM Serving: vLLM with GCS FUSE on GKE

Model serving on GKE utilizes **vLLM** hosted on GPU node pools managed by **KServe**:

### A. Accelerated Model Loading with GCS FUSE CSI Driver
Traditional Kubernetes deployments suffer from 15–20 minute pod cold starts as 70B parameter models (140 GB) are downloaded from object storage. 
On GKE, the native **Cloud Storage FUSE CSI Driver** streams model weights directly into GPU memory with local NVMe caching, reducing pod startup to under **45 seconds**:

```yaml
apiVersion: "serving.kserve.io/v1beta1"
kind: "InferenceService"
metadata:
  name: "llama-3-70b-instruct"
  namespace: "ai-serving"
  annotations:
    gke-gcsfuse/volumes: "true"
    gke-gcsfuse/memory-limit: "8Gi"
    gke-gcsfuse/cpu-limit: "4"
spec:
  predictor:
    minReplicas: 1
    maxReplicas: 6
    scaleTarget: 15 # Concurrent requests per replica
    scaleMetric: concurrency
    model:
      modelFormat:
        name: vLLM
      resources:
        limits:
          nvidia.com/gpu: "4" # 4x A100 80GB (Tensor Parallelism = 4)
          memory: "128Gi"
          cpu: "16"
        requests:
          nvidia.com/gpu: "4"
          memory: "64Gi"
          cpu: "8"
      args:
        - "--model=/mnt/models/hub/Meta-Llama-3-70B-Instruct"
        - "--tensor-parallel-size=4"
        - "--max-model-len=16384"
        - "--gpu-memory-utilization=0.92"
        - "--enable-prefix-caching"
      volumeMounts:
        - name: gcs-model-store
          mountPath: /mnt/models
          readOnly: true
    volumes:
      - name: gcs-model-store
        csi:
          driver: gcsfuse.csi.storage.gke.io
          readOnly: true
          volumeAttributes:
            bucketName: data-platform-models-prod
            mountOptions: "implicit-dirs,file-cache:max-size-mb:200000"
```

---

## 3. Native GKE Sandbox for Untrusted Agent Tool Execution

AI agents routinely generate dynamic Python scripts (data transformation, calculations, visualization). Executing untrusted code inside standard containers risks container breakouts and internal network probing.

On GKE, sandboxing is managed natively via **GKE Sandbox**:
1. **gVisor User-Space Kernel:** Intercepts application system calls, isolating the host Linux kernel from untrusted agent code.
2. **Metadata Server Protection:** GKE Workload Identity automatically blocks access to the Google Cloud Metadata server (`169.254.169.254`), preventing agents from stealing cluster credentials.
3. **Strict Network & Resource Limits:** Ephemeral pods are configured with 512 MB RAM, 1 CPU, 15-second timeouts, and zero egress except to the local MCP Gateway.

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: ephemeral-agent-sandbox-0412
  namespace: agent-sandboxes
  annotations:
    sandbox.gke.io/type: gvisor # Activates GKE Sandbox kernel isolation
spec:
  restartPolicy: Never
  nodeSelector:
    sandbox.gke.io/type: gvisor
  containers:
  - name: python-tool-runner
    image: internal-registry.corp/data-platform/agent-python-sandbox:3.11-slim
    command: ["python", "-c", "import sys; ..."]
    resources:
      limits:
        memory: "512Mi"
        cpu: "1000m"
    securityContext:
      readOnlyRootFilesystem: true
      allowPrivilegeEscalation: false
      runAsNonRoot: true
      runAsUser: 10001
      capabilities:
        drop: ["ALL"]
```

---

## 4. 3-Tier Agent Memory Subsystem on Google Cloud

```mermaid
flowchart LR
    Agent["AI Agent Worker"]:::purple

    subgraph L1["L1: Working Memory"]
        Redis["Google Cloud Memorystore / Redis on GKE\n- Sub-millisecond latency\n- Active session buffer & scratchpad\n- TTL: 24 Hours"]:::warning
    end

    subgraph L2["L2: Semantic Memory"]
        Postgres["PostgreSQL + pgvector (CloudNativePG on Hyperdisk)\n- Sub-40ms vector similarity (HNSW)\n- User persona embeddings & long-term knowledge\n- Permanent persistence"]:::warning
    end

    subgraph L3["L3: Episodic / Audit Memory"]
        Iceberg["Apache Iceberg on GCS (gs://lakehouse-data/)\n- Full agent reasoning trajectories & tool logs\n- Audit compliance under Decree 13 PDPD\n- Fine-tuning & offline RLHF/DPO datasets"]:::success
    end

    Agent <-->|"Active Turn (<2ms)"| L1
    Agent <-->|"Semantic Search (<40ms)"| L2
    Agent -->|"Async Event Telemetry"| L3

    %% Subgraph Styling (Transparent & Dual-Mode Friendly)
    style L1 fill:none,stroke:#f59e0b,stroke-width:1.5px,stroke-dasharray: 4 4,color:#fcd34d
    style L2 fill:none,stroke:#f59e0b,stroke-width:1.5px,stroke-dasharray: 4 4,color:#fcd34d
    style L3 fill:none,stroke:#10b981,stroke-width:1.5px,stroke-dasharray: 4 4,color:#6ee7b7

    %% Link Styling
    linkStyle default stroke:#64748b,stroke-width:2px;

    %% Universal Dual-Mode High-Contrast Palette
    classDef success fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#ffffff;
    classDef warning fill:#451a03,stroke:#f59e0b,stroke-width:2px,color:#ffffff;
    classDef purple  fill:#2e1065,stroke:#a855f7,stroke-width:2px,color:#ffffff;
```

### PostgreSQL + `pgvector` Deployment on GKE
Managed via the **CloudNativePG Operator** using Google Cloud **Hyperdisk Balanced**:
```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: agent-vector-store
  namespace: ai-platform
spec:
  instances: 3
  storage:
    storageClass: hyperdisk-balanced
    size: 250Gi
  postgresql:
    parameters:
      shared_buffers: "16GB"
      work_mem: "64MB"
      maintenance_work_mem: "2GB"
    shared_preload_libraries:
      - "vector"
```

---

## 5. Model Context Protocol (MCP) Gateway on GKE

The **Model Context Protocol (MCP)** standardizes tool integration between AI agents and the data platform:
* **Exposed Data Platform Tools:**
  1. `execute_iceberg_sql`: Routes parameterized queries to Trino with automated partition pruning checks.
  2. `search_vector_knowledge`: Performs cosine similarity queries on PostgreSQL `pgvector`.
  3. `inspect_catalog_metadata`: Reads table documentation and schema definitions from Lakekeeper.
* **Security & Rate Limiting:** Enforces token-bucket rate limiting, checks GKE Workload Identity credentials, and logs all tool executions to the L3 Iceberg audit log.
