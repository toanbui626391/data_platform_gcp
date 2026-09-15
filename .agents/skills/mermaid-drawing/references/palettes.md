# Mermaid Dual-Mode Color Palettes Reference

This reference provides pre-calculated color palettes designed to satisfy WCAG AA contrast standards (> 4.5:1 ratio) against both pure white (`#FFFFFF`) and dark slate/black (`#0F172A` / `#000000`) canvases.

---

## Palette 1: Modern Dark Jewel (Recommended Default)

*Theme Concept:* Rich, deep jewel fills with bright neon-accented borders and crisp white typography. Unbeatable readability in all environments.

```mermaid
%% Copy-Paste Palette 1 %%
classDef blue   fill:#1e3a8a,stroke:#60a5fa,stroke-width:2px,color:#ffffff;
classDef emerald fill:#064e3b,stroke:#34d399,stroke-width:2px,color:#ffffff;
classDef amber  fill:#451a03,stroke:#fbbf24,stroke-width:2px,color:#ffffff;
classDef rose   fill:#4c0519,stroke:#fb7185,stroke-width:2px,color:#ffffff;
classDef purple fill:#2e1065,stroke:#c084fc,stroke-width:2px,color:#ffffff;
classDef cyan   fill:#083344,stroke:#22d3ee,stroke-width:2px,color:#ffffff;
classDef slate  fill:#1e293b,stroke:#94a3b8,stroke-width:1.5px,color:#f8fafc;
```

---

## Palette 2: Crisp Dual-Border (Subtle Background)

*Theme Concept:* Very dark slate backgrounds with high-contrast colored borders and matching colored text headers.

```mermaid
%% Copy-Paste Palette 2 %%
classDef borderBlue   fill:#0f172a,stroke:#3b82f6,stroke-width:2px,color:#93c5fd;
classDef borderGreen  fill:#0f172a,stroke:#10b981,stroke-width:2px,color:#6ee7b7;
classDef borderAmber  fill:#0f172a,stroke:#f59e0b,stroke-width:2px,color:#fcd34d;
classDef borderRed    fill:#0f172a,stroke:#ef4444,stroke-width:2px,color:#fca5a5;
classDef borderPurple fill:#0f172a,stroke:#a855f7,stroke-width:2px,color:#d8b4fe;
```

---

## Palette 3: High-Contrast Pastels (Dark Font Standard)

*Caution:* If you use light pastel fills, text MUST be hardcoded to near-black (`#0f172a` or `#000000`). Never use default or white text with light fills.

```mermaid
%% Copy-Paste Palette 3 (Always use dark text: #0f172a) %%
classDef pastelBlue   fill:#dbeafe,stroke:#2563eb,stroke-width:2px,color:#0f172a;
classDef pastelGreen  fill:#d1fae5,stroke:#059669,stroke-width:2px,color:#0f172a;
classDef pastelAmber  fill:#fef3c7,stroke:#d97706,stroke-width:2px,color:#0f172a;
classDef pastelRose   fill:#ffe4e6,stroke:#e11d48,stroke-width:2px,color:#0f172a;
classDef pastelPurple fill:#f3e8ff,stroke:#7c3aed,stroke-width:2px,color:#0f172a;
```

---

## Subgraph (Container) Standards

To ensure subgraphs look intentional and never obscure diagram contents:

```mermaid
%% Default Subgraph Styler %%
style SubgraphName fill:none,stroke:#64748b,stroke-width:1.5px,stroke-dasharray: 4 4,color:#94a3b8

%% Highlighted / Security Perimeter Subgraph %%
style PerimeterName fill:none,stroke:#f43f5e,stroke-width:2px,stroke-dasharray: 6 3,color:#fb7185
```
