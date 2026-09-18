# Architecture Q&A: Vietnam Decree 13 PDPD, Cascading RTBF & Crypto-Shredding

---

## 1. Problem Understanding

Under Vietnam's **Personal Data Protection Decree (Decree 13/2023/ND-CP, Articles 9 & 16)** and banking compliance mandates (State Bank of Vietnam), data subjects possess the **Right to Erasure / Right to be Forgotten (RTBF)**. The platform operator is legally mandated to permanently erase or render irretrievable all personal data of a requesting subject across the entire infrastructure within **72 hours**.

This mandate collides violently with modern big data architectures built on the premise of **immutability**:

```mermaid
flowchart TB
    subgraph LegalMandate["Decree 13/2023/ND-CP Legal Mandate"]
        RTBF["Customer Submits RTBF Request\n(Must erase ALL PII within 72 hours)"]
        Penalties["Non-Compliance Penalty: Up to 5% Annual Revenue\n+ Suspension of Banking Operations"]
        RTBF --> Penalties
    end

    subgraph ArchitectureCollisions["The Big Data Immutability Paradox"]
        KafkaCollision["1. Kafka Logs: Immutable write-ahead append log on disk"]
        BronzeCollision["2. Iceberg Bronze: Millions of immutable Parquet files on GCS"]
        VectorCollision["3. pgvector: Dense HNSW graph contains semantic embeddings of PII"]
        LLMCollision["4. AI Agent & vLLM: Prompt context in Redis L1 & GPU KV prefix caches"]
        LineageCollision["5. OpenMetadata: Lineage graphs track historical data provenance"]
    end

    RTBF --> ArchitectureCollisions

    classDef danger fill:#4c0519,stroke:#f43f5e,stroke-width:2px,color:#ffffff;
    classDef warning fill:#451a03,stroke:#f59e0b,stroke-width:2px,color:#ffffff;
    classDef primary fill:#1e3a8a,stroke:#3b82f6,stroke-width:2px,color:#ffffff;

    RTBF:::danger
    Penalties:::danger
    KafkaCollision:::warning
    BronzeCollision:::warning
    VectorCollision:::warning
    LLMCollision:::warning
    LineageCollision:::warning
```

### The 4 Major Engineering Traps of Naive Deletion:
1. **The Petabyte Rewrite Trap:** Executing `DELETE FROM bronze WHERE user_id = 'X'` on petabytes of append-only Bronze Parquet files requires rewriting months of historical files, costing thousands in cloud compute and corrupting Iceberg snapshot time-travel.
2. **The Vector Leakage Trap:** Deleting text from a relational database while leaving embeddings in `pgvector` violates Decree 13. Research shows high-dimensional vector embeddings can be inverted to reconstruct sensitive textual attributes.
3. **The AI Inference Cache Trap:** If an AI Agent recently processed a user's financial record, the prompt tokens remain cached in **Redis L1** and **vLLM's PagedAttention KV Prefix Cache**, continuing to serve personalized context after the user was supposedly deleted.
4. **The Broken Audit Lineage Trap:** Naively wiping records from metadata catalogs destroys historical data provenance, leaving the platform unable to prove to banking auditors that the data ever existed or that the deletion was executed lawfully.

---

## 2. Solution & Why the Solution Works

Our architecture resolves the immutability paradox through a **4-Stage Cascading Deletion Orchestrator**:

```mermaid
flowchart TB
    User["Customer RTBF Request"] --> Orchestrator["RTBF Compliance Orchestrator (GKE Job)"]

    subgraph Stage1["Stage 1: Crypto-Shredding (Immediate: < 1 second)"]
        Orchestrator --> KMS["Google Cloud KMS"]
        KMS -->|"Destroy User's Data Encryption Key (DEK)"| KMSDead["DEK Erased Permanently\nKey: dek_user_9921 -> DESTROYED"]
        KMSDead -.->|"Rendered Unrecoverable Ciphertext"| KafkaStore["Kafka Append Logs\n(Payload encrypted with dead DEK)"]
        KMSDead -.->|"Rendered Unrecoverable Ciphertext"| BronzeGCS["Iceberg Bronze Parquet\n(Payload encrypted with dead DEK)"]
    end

    subgraph Stage2["Stage 2: Iceberg Silver/Gold Metadata Deletion (< 1 hour)"]
        Orchestrator --> Lakekeeper["Lakekeeper REST Catalog"]
        Lakekeeper -->|"Issue Position Delete Snapshot"| SilverGold["Silver & Gold Iceberg Tables\n(MOR Position Deletes Exclude User)"]
        SilverGold -->|"Scheduled Spark Compaction"| CompactedParquet["Physically Purged Parquet Files\n(Old Snapshots Expired)"]
    end

    subgraph Stage3["Stage 3: Vector & AI Cache Purge (< 5 minutes)"]
        Orchestrator --> PGVector["PostgreSQL pgvector\n(DELETE + VACUUM HNSW Index)"]
        Orchestrator --> RedisL1["Redis L1 Session Buffer\n(FLUSH session:user_9921:*)"]
        Orchestrator --> vLLMCache["vLLM Model Serving\n(Evict KV Prefix Cache)"]
    end

    subgraph Stage4["Stage 4: Compliance Tombstone in OpenMetadata (< 72 hours)"]
        Orchestrator --> OpenMetadata["OpenMetadata Governance Graph"]
        OpenMetadata --> ComplianceCert["Issue Cryptographic Erasure Certificate\n(Lineage preserved, PII replaced with TOMBSTONE)"]
    end

    classDef primary fill:#1e3a8a,stroke:#3b82f6,stroke-width:2px,color:#ffffff;
    classDef success fill:#064e3b,stroke:#10b981,stroke-width:2px,color:#ffffff;
    classDef warning fill:#451a03,stroke:#f59e0b,stroke-width:2px,color:#ffffff;
    classDef danger  fill:#4c0519,stroke:#f43f5e,stroke-width:2px,color:#ffffff;
    classDef cyan    fill:#083344,stroke:#06b6d4,stroke-width:2px,color:#ffffff;

    Orchestrator:::danger
    KMS:::primary
    KMSDead:::danger
    Lakekeeper:::cyan
    PGVector:::warning
    RedisL1:::warning
    vLLMCache:::warning
    OpenMetadata:::cyan
    CompactedParquet:::success
    ComplianceCert:::success
```

---

### Part A: Crypto-Shredding (Envelope Encryption at the Ingestion Edge)

To delete data from immutable storage (Kafka logs and Bronze Iceberg tables) without rewriting petabytes of data, we implement **Crypto-Shredding**:

#### 1. Ingestion Edge Encryption (Flink & Debezium)
When data enters the streaming fabric:
* Sensitive PII fields (Vietnamese National ID / CCCD, phone, banking numbers, addresses) are encrypted using a **unique per-user Data Encryption Key (DEK)**:
  $$\text{Ciphertext} = \text{AES-256-GCM}(\text{PII\_Data}, \text{DEK}_{\text{user\_id}})$$
* The DEK is generated and protected under a Key Encryption Key (KEK) managed in **Google Cloud KMS (CMEK)**.
* Encrypted ciphertext is written to Kafka and Iceberg Bronze.

#### 2. The RTBF Trigger: Instant Mathematical Erasure ($< 1\text{s}$)
When an RTBF erasure request is approved:
1. The orchestrator calls Google Cloud KMS API:
   ```bash
   gcloud kms keys versions destroy 1 \
     --key="dek_user_9921" \
     --keyring="pdpd-user-keys" \
     --location="asia-southeast1"
   ```
2. **Why This Works:**
   * The actual bytes in Kafka and Bronze Parquet files remain untouched on GCS, but their payload is now **mathematically unrecoverable random noise**.
   * Under Vietnam Decree 13 Article 16 and international cryptographic standards, **destroying the unique decryption key constitutes permanent, irreversible erasure**.
   * Zero GCS Parquet files are rewritten; zero Kafka topics are modified. Cost: $\$0.00$. Time: $< 1\text{ second}$.

---

### Part B: Lakehouse Silver & Gold Position Deletes & Physical Compaction

For clean queryable tables (Silver customer entities and Gold analytical marts), data is physically removed via Iceberg’s **Merge-On-Read (MOR)** lifecycle:

1. **Immediate Metadata Exclusion ($< 5\text{m}$):**
   ```sql
   DELETE FROM silver.customer_profiles WHERE user_id = 'user_9921';
   ```
   Iceberg writes a lightweight **Position Delete file**. Immediately, any downstream Trino, Spark, or BI query ignores this user's records.
2. **Physical Storage Reclamation (Compaction Daemon):**
   * The scheduled LakeOps compaction job rewrites the affected data files, physically omitting the deleted user records:
     ```sql
     CALL lakehouse.system.rewrite_data_files(
       table => 'silver.customer_profiles',
       strategy => 'binpack'
     );
     CALL lakehouse.system.expire_snapshots(
       table => 'silver.customer_profiles',
       older_than => CURRENT_TIMESTAMP - INTERVAL '3' DAY
     );
     ```
   * Old Parquet files containing the deleted rows are purged from GCS within the 72-hour regulatory window.

---

### Part C: Vector DB (`pgvector`) & AI Context Cache Purging

Because vector embeddings encode semantic meaning that can be inverted to reveal personal attributes, they must be aggressively purged:

#### 1. PostgreSQL `pgvector` HNSW Hard Delete
```sql
-- 1. Delete matching vector chunks
DELETE FROM rag_knowledge_chunks 
WHERE document_id LIKE 'user_9921_%' 
   OR metadata->>'user_id' = 'user_9921';

-- 2. Prune dead graph links and reclaim memory
VACUUM (INDEX_CLEANUP ON) rag_knowledge_chunks;
```
`VACUUM (INDEX_CLEANUP ON)` removes dead vector tuples from HNSW index pages and repairs graph routing edges.

#### 2. Redis L1 & vLLM Prefix Cache Flush
* **Redis L1:** Delete active user sessions and cached thought scratchpads:
  ```bash
  redis-cli EVAL "return redis.call('del', unpack(redis.call('keys', 'session:*:user_9921:*')))" 0
  ```
* **vLLM KV Prefix Cache Eviction:** Send an eviction webhook to KServe / vLLM to reset prompt token caches:
  ```bash
  curl -X POST http://vllm-service.ai-serving.svc:8000/v1/cache/clear_prefix
  ```
  This prevents the LLM from completing prompts with cached in-flight conversational PII.

---

### Part D: Preserving Lineage in OpenMetadata without PII Contamination

Auditors from the State Bank of Vietnam (SBV) require proof that the deletion occurred without breaking operational data pipelines:

1. **The Compliance Tombstone Pattern:**
   * Instead of deleting the entity from OpenMetadata (which destroys historical pipeline lineage), OpenMetadata replaces the user entity with a **Cryptographic Tombstone**:
   ```json
   {
     "entity_id": "sha256(user_9921 + salt)",
     "status": "PURGED_DECREE_13",
     "erasure_timestamp": "2026-09-18T15:00:00Z",
     "legal_basis": "Decree 13/2023/ND-CP Article 9/16",
     "kms_key_destroyed": "dek_user_9921",
     "purged_subsystems": ["kafka", "iceberg_bronze", "iceberg_silver", "pgvector", "redis_l1"]
   }
   ```
2. **Lineage Integrity:**
   * Downstream lineage graphs in OpenMetadata remain completely connected and functional for regulatory audits.
   * All PII attributes (`customer_name`, `cccd`, `phone`) are replaced with `[REDACTED_DECREE13_RTBF]`.

---

## 3. Summary: Actual Issues Solved by Architectural Design Choices

| Category | Actual Root Issue / Failure Mode | Architectural Design Choice | Solved Outcome & Guarantees |
| :--- | :--- | :--- | :--- |
| **Immutable Storage Erasure** | **Petabyte Rewrite Bottleneck:** Deleting rows from raw Kafka logs and Iceberg Bronze Parquet files requires rewriting historical datasets at unsustainable compute cost. | **Crypto-Shredding via Cloud KMS:** Sensitive PII is encrypted with per-user DEKs; KMS destroys the DEK upon RTBF request. | Instant mathematical erasure ($< 1\text{s}$) across all historical logs and backups with **zero data rewritten**; satisfies Decree 13 Article 16. |
| **Lakehouse State Deletion** | **Active Analytical Leakage:** Deleted users continue appearing in Trino SQL dashboards during long compaction intervals. | **Two-Stage Iceberg Deletion (MOR Position Delete + Compaction):** Metadata delete file takes effect immediately; physical rewrite runs within 72h. | Trino queries exclude user data in **$< 5\text{ms}$**; physical Parquet files are compacted and old snapshots expired within 72h SLA. |
| **Semantic Vector Inversion** | **Vector Memory Leakage:** Dense vector embeddings left in `pgvector` can be mathematically inverted to reconstruct PII. | **SQL Hard Delete + `VACUUM (INDEX_CLEANUP ON)`:** Hard row deletion followed by HNSW graph unlinking. | Permanent elimination of vector representation; HNSW index memory is reclaimed without graph degradation. |
| **AI Serving Context Cache** | **Cached Prompt Leakage:** LLM generates deleted user PII due to cached KV prefixes in vLLM or active Redis L1 scratchpads. | **Redis Session Flush + vLLM KV Prefix Invalidation:** Webhook clears Redis session keys and resets vLLM prompt caches. | Zero prompt cache contamination; conversational agents cannot recall deleted user context. |
| **Audit & Lineage Governance** | **Lineage Corruption vs. PII Retention:** Deleting metadata breaks audit graphs; keeping metadata violates Decree 13 data retention limits. | **OpenMetadata Compliance Tombstones:** PII attributes are redacted, replaced by an immutable cryptographic erasure certificate. | $100\%$ verifiable regulatory compliance for banking auditors; end-to-end lineage remains unbroken. |
