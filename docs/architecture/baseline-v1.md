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