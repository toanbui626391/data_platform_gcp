# Dual-Mode Sequence Diagram Example

In Mermaid `sequenceDiagram`, participant boxes and notes can also be styled for light and dark mode using directives or CSS class overrides.

---

## 1. Sequence Diagram Example with Theming

```mermaid
%%{init: {
  'theme': 'base',
  'themeVariables': {
    'actorBkg': '#1e293b',
    'actorBorder': '#38bdf8',
    'actorTextColor': '#ffffff',
    'signalColor': '#94a3b8',
    'signalTextColor': '#f8fafc',
    'labelBoxBkgColor': '#1e3a8a',
    'labelBoxBorderColor': '#3b82f6',
    'labelTextColor': '#ffffff',
    'loopTextColor': '#f8fafc',
    'noteBkgColor': '#064e3b',
    'noteBorderColor': '#10b981',
    'noteTextColor': '#ffffff',
    'activationBkgColor': '#2e1065',
    'activationBorderColor': '#a855f7'
  }
}}%%
sequenceDiagram
    autonumber
    actor User as User / Client
    participant Agent as AI Agent Worker
    participant Tool as gVisor Sandbox
    participant Lake as Trino & Iceberg

    User->>Agent: Prompt: Analyze Q3 Credit Anomalies
    activate Agent
    Note over Agent: PII Masked via Local Privacy Proxy
    Agent->>Tool: Dispatch dynamic Python code
    activate Tool
    Tool->>Lake: Query Iceberg Gold Partition
    Lake-->>Tool: Return aggregated rows
    Tool-->>Agent: Code output & metrics
    deactivate Tool
    Agent-->>User: Synthesized response & visualization
    deactivate Agent
```

---

## 2. Key Takeaways for Sequence Diagrams
1. Always inject the `%%{init: ...}%%` block at the top of sequence diagrams when color is desired.
2. Set `actorBkg: '#1e293b'` and `actorTextColor: '#ffffff'` to prevent black-on-dark or white-on-white text collisions.
3. Use a vibrant `actorBorder` (such as `#38bdf8` or `#818cf8`) so participants stand out prominently on dark and light canvas themes.
