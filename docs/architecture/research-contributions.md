# Proposed Research Contributions

## Contribution 1 — Dual-Dimensional Data-Lake Architecture

The project proposes an architecture combining two complementary
dimensions:

1. Functional layers describing system responsibilities:
   acquisition, ingestion, data management, intelligence, and
   consumption.
2. Medallion zones describing data maturity:
   Bronze, Silver, and Gold.

The functional layers explain what the system performs, while the
Medallion zones explain the quality and readiness state of stored
data.

---

## Contribution 2 — Source-Aware Heterogeneous Bronze Ingestion

The prototype supports source-specific ingestion adapters while
preserving a shared Bronze storage contract.

Each Bronze event contains:

- An immutable source payload.
- Source-system metadata.
- Source format and schema version.
- Ingestion timestamp.
- Kafka topic, partition, and offset.
- A unique event identifier.
- A processing-run identifier.

This makes the Bronze zone replayable, source-preserving, and
auditable.

---

## Contribution 3 — Canonical Agricultural Silver Models

Different source schemas are mapped into standardized canonical
representations.

The prototype currently defines:

- A canonical field-observation schema.
- A canonical weather-observation schema.
- An integrated field–weather Silver dataset.

This separates source-specific structure from downstream analytics
and reduces coupling between ingestion and consumption.

---

## Contribution 4 — Quality-Aware Accepted and Quarantine Zones

The Silver layer separates trusted records from rejected records.

Rejected records are not silently removed. They are stored with:

- The original Bronze record.
- The failure stage.
- Validation errors.
- Bronze object location.
- Processing-run identifiers.
- Quarantine timestamp.

Quality reports measure acceptance, quarantine, uniqueness, and
failure categories.

---

## Contribution 5 — Run-Level Metadata and Multi-Parent Lineage

Every bounded pipeline job generates a run manifest.

Transformation jobs generate lineage connecting:

- Bronze runs to Silver runs.
- Field and weather Silver runs to the integration run.
- Integration runs to Gold analytical products.

The integration job supports multiple parent runs, which represents
heterogeneous dataset provenance explicitly.

---

## Contribution 6 — Purpose-Specific AI-Enriched Gold Products

The Gold layer contains named analytical products rather than one
generic output file.

Current products include:

- Scored field observations.
- Field anomaly alerts.
- Integrated field–weather scores.
- Integrated anomaly alerts.
- Anomaly-detection summaries.

Each output records the model name, model version, processing run,
and parent dataset.

---

## Contribution 7 — Architecture Evaluation Framework

The prototype automatically extracts:

- Processing durations.
- Record counts.
- Integration coverage.
- Data-quality metrics.
- Anomaly counts.
- Storage sizes.
- Manifest availability.
- Lineage availability.

This supports comparison between the basic baseline Medallion
pipeline and the proposed dual-dimensional architecture.

---

## Scope of the Contribution

The contribution is architectural and prototype-oriented.

It does not claim:

- Production-scale performance.
- Clinical or agronomic validation of detected anomalies.
- A complete governance platform.
- A replacement for enterprise metadata catalogs.
- That synthetic weather observations are real measured data.