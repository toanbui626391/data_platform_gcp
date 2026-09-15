# Data Platform Operational Instructions & SOP Suite

This directory contains standard operating procedures (SOPs), runbooks, and implementation guides for **Data Platform Engineers** operating the **Unified GCP AI & Data Platform**.

---

## Instruction Index

| Document | Title | Scope & Objectives |
| :--- | :--- | :--- |
| **[01: Platform-as-a-Product & GitOps-First](file:///c:/Users/ToanBX/dev/personal/data_platform_gcp/docs/instructions/01_platform_as_a_product_and_gitops.md)** | **Zero Console Tweaks Mandate** | • Two-Tier GitOps Architecture (Terraform + ArgoCD)<br>• Repository Layout & Separation of Concerns<br>• Resource Provisioning & Upgrades Step-by-Step Workflows<br>• Drift Detection & Emergency Break-Glass Protocols<br>• Policy-as-Code Technical Enforcement |

---

## Operational Mandates Summary

1. **Zero Manual Edits in Production:** No human creates GCS buckets, edits Kubernetes pods, or tweaks VPCs manually. Everything is driven by Git.
2. **Automated Drift Rollback:** ArgoCD and Terraform CI enforce declarative state, rolling back out-of-band changes.
3. **Least-Privilege Identities:** Zero long-lived JSON service account keys; all access uses GKE Workload Identity Federation.
