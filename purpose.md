# Purpose

This project builds a real-time data pipeline for a video streaming platform. 
It ingests large-scale user interaction events (video watches and likes) from JSON log files stored in AWS S3, 
streams them through Apache Kafka, processes them with Apache Flink for enrichment and aggregation, 
and stores results in Apache HBase for fast querying.

Goals:
- Handle millions of daily events.
- Store detailed user histories and aggregated video statistics.
- Enable future features like real-time recommendations and fraud detection.

Technologies:
- AWS S3 (object store for raw logs)
- Apache Kafka (event streaming)
- Apache Flink (real-time processing)
- Apache HBase (hot data storage)
- Docker Compose (or Kubernetes later)
