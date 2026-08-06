# Research Questions

## Main Research Question

How can a dual-dimensional data-lake architecture combining
functional processing layers with Medallion data-maturity zones
improve the integration, quality management, traceability, and
AI-assisted analysis of heterogeneous agricultural data?

---

## RQ1 — Heterogeneous Data Integration

To what extent can the proposed architecture integrate
heterogeneous agricultural data sources that differ in format,
schema, and ingestion pattern?

### Evidence

- Number of source systems.
- Number of source formats.
- Number of Kafka topics.
- Field–weather integration match rate.
- Number of successfully integrated records.
- Number of unmatched records.
- Canonical-schema validation results.

### Current prototype

The prototype currently integrates:

1. A CSV crop-field dataset sent as a batch simulated as a stream.
2. A JSON weather-station dataset sent as sensor-style events.

Both sources are standardized independently before integration.

---

## RQ2 — Data Quality Management

How effectively does the proposed architecture identify, separate,
and document invalid agricultural records during the transition
from Bronze to Silver?

### Evidence

- Input-record count.
- Accepted-record count.
- Quarantined-record count.
- Acceptance rate.
- Quarantine rate.
- Duplicate-record count.
- Source-schema failures.
- Canonical-mapping failures.
- Canonical-schema failures.
- Overall quality score.

### Current prototype

Invalid records are preserved in a quarantine zone together with
their original content, source location, failed stage, and failure
reasons.

---

## RQ3 — Traceability and Reproducibility

Can the proposed architecture provide complete traceability from
Gold analytical products back to their Silver and Bronze parent
runs?

### Evidence

- Percentage of completed jobs with manifests.
- Percentage of transformation jobs with lineage records.
- Number of parent runs recorded per job.
- Ability to select historical Bronze or Silver runs explicitly.
- Ability to verify that declared MinIO objects still exist.
- Ability to reproduce outputs from selected parent runs.

### Current prototype

Each processing execution receives a unique run identifier and
produces a manifest. Silver, integration, and Gold jobs also
produce explicit lineage records.

---

## RQ4 — Architectural Overhead

What execution-time and storage overhead is introduced by
metadata, canonical schemas, quarantine handling, quality reports,
and lineage management?

### Evidence

- Processing duration per stage.
- Total processing duration.
- Data-output storage size.
- Metadata storage size.
- Number of generated metadata objects.
- Storage overhead relative to the baseline prototype.

### Interpretation rule

Additional overhead is acceptable only when it is accompanied by
measurable gains in data quality, integration, reproducibility, or
traceability.

---

## RQ5 — AI-Ready Data Products

Can trusted and integrated Silver data support reproducible
AI-based agricultural anomaly detection and purpose-specific Gold
data products?

### Evidence

- Number of records scored.
- Number and percentage of anomalies.
- Availability of model name and version.
- Availability of feature definitions.
- Number of generated alerts.
- Number of alerts enriched with weather context.
- Gold-to-Silver lineage availability.

### Limitation

Isolation Forest identifies statistically unusual observations.
It does not prove that every detected observation represents a
true agricultural fault or harmful condition.