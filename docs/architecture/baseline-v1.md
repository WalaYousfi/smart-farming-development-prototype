## system description
CSV
→ Kafka
→ MinIO Bronze
→ pandas Silver
→ Isolation Forest
→ MinIO Gold


## system limitations
one source;
one format;
limited metadata;
no quarantine;
no canonical integration;
limited lineage;
one generic Gold output.






# Baseline Prototype (V1)

## Objective

## Architecture

## Components

## Technologies

## Pipeline

Producer
↓

Kafka
↓

Consumer
↓

MinIO

## Capabilities

- Single source
- Single pipeline
- No metadata
- No lineage
- No manifests
- Basic anomaly detection

## Limitations

- One data source only
- No heterogeneous integration
- No canonical schemas
- No quality reports
- No experiment framework