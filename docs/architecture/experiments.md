# Experimental Evaluation Plan

## Evaluation Objective

The experiments evaluate whether the proposed architecture provides
measurable benefits in heterogeneous integration, data quality,
traceability, reproducibility, and AI-ready data preparation while
maintaining acceptable processing and storage overhead.

---

# Experiment 1 — End-to-End Functional Validation

## Objective

Verify that all pipeline stages complete successfully.

## Input

- 500 crop-field observations.
- 500 synthetic weather observations.

## Procedure

1. Send both sources through separate Kafka topics.
2. Store source-aware Bronze events in MinIO.
3. Create canonical Field and Weather Silver datasets.
4. Integrate both Silver datasets.
5. Run Gold anomaly detection.
6. verify all manifests and lineage records.

## Metrics

- Records received.
- Records stored in Bronze.
- Records accepted into Silver.
- Integrated records.
- Gold records.
- Missing or lost records.

## Success criterion

The number of expected records is preserved across all stages,
excluding records deliberately moved into quarantine.

---

# Experiment 2 — Heterogeneous Integration Coverage

## Objective

Measure the ability to integrate two independent source schemas.

## Independent variables

- Number of field observations.
- Number of weather observations.
- Availability of shared integration keys.

## Metrics

- Matched records.
- Unmatched records.
- Match rate.
- Integration output count.
- Number of parent Silver runs.

## Primary expected result

With the controlled full-coverage dataset:

- Field records: 500.
- Weather records: 500.
- Matched records: 500.
- Match rate: 1.0.

---

# Experiment 3 — Data-Quality and Quarantine Validation

## Objective

Evaluate whether invalid data is identified and preserved with an
explanation.

## Controlled invalid cases

- Missing mandatory identifier.
- Numeric conversion failure.
- Invalid timestamp.
- Out-of-range soil moisture.
- Duplicate event identifier.

## Metrics

- Accepted records.
- Quarantined records.
- Failure-stage distribution.
- Source-schema failures.
- Mapping failures.
- Canonical-schema failures.
- Duplicate-record count.

## Success criterion

Every controlled invalid record must be quarantined at the expected
stage and remain traceable to its Bronze input.

---

# Experiment 4 — Traceability Verification

## Objective

Verify the path from Gold outputs back to their parent datasets.

## Procedure

1. Select one completed Gold run.
2. Read its lineage record.
3. Identify the parent integration or Silver run.
4. Follow the chain to Field and Weather Silver runs.
5. Follow both source paths to Bronze runs.
6. verify all declared MinIO objects.

## Metrics

- Traceability status.
- Lineage depth.
- Number of parent runs.
- Existing declared objects.
- Missing declared objects.
- Percentage of jobs with lineage.

## Success criterion

All declared objects exist and the complete source-to-Gold chain can
be reconstructed.

---

# Experiment 5 — Processing-Time Evaluation

## Objective

Measure the execution time of each bounded processing stage.

## Stages

- Field Silver transformation.
- Weather Silver transformation.
- Field–weather integration.
- Field-only Gold anomaly detection.
- Integrated Gold anomaly detection.

## Procedure

Execute each experimental configuration at least five times.

## Metrics

- Individual run duration.
- Mean duration.
- Median duration.
- Minimum duration.
- Maximum duration.
- Standard deviation.

## Experimental condition

All runs must use the same machine, Docker configuration, dataset,
and background-workload conditions.

---

# Experiment 6 — Storage-Overhead Evaluation

## Objective

Measure the storage introduced by data maturity zones and metadata.

## Categories

- Bronze data objects.
- Accepted Silver data.
- Quarantine data.
- Integrated Silver data.
- Gold analytical data.
- Gold data products.
- Manifests.
- Lineage records.
- Quality reports.

## Metrics

- Size in bytes.
- Size in megabytes.
- Object count.
- Metadata-to-data storage ratio.
- Proposed-V2-to-baseline storage ratio.

---

# Experiment 7 — Baseline Versus Proposed Architecture

## Baseline

CSV → Kafka → Bronze → basic Silver → Isolation Forest → Gold.

## Proposed architecture

Heterogeneous sources → source-aware ingestion → metadata-enriched
Bronze → canonical source Silvers → quarantine and quality reports
→ integrated Silver → AI-enriched Gold products → lineage.

## Comparison dimensions

- Source count.
- Format count.
- Canonical schemas.
- Quarantine support.
- Quality reporting.
- Manifest coverage.
- Lineage coverage.
- Integration coverage.
- Processing time.
- Storage size.
- Number of usable Gold products.

## Important interpretation

The proposed architecture is expected to consume more time and
storage because it performs more responsibilities. Its value must
therefore be assessed through improved capabilities, not only raw
speed.

---

# Experiment 8 — AI Output Comparison

## Objective

Compare field-only and integrated Gold anomaly outputs.

## Metrics

- Total anomalies.
- Anomaly overlap.
- Anomalies unique to each configuration.
- Rank correlation between anomaly scores.
- Weather-enriched anomaly count.
- Processing-time difference.
- Storage-size difference.

## Current limitation

The initial integrated model uses field features for scoring and
weather information as enrichment context. A later experiment may
include weather features after confirming complete and independent
weather coverage.