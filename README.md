# Real-Time E-Commerce Data Engineering Project using Apache Kafka, Python & Snowflake

---

## 📌 Project Overview

This project simulates a real-time e-commerce event pipeline, where user activity data is continuously ingested, validated, and made analytics-ready; A production-style, time data engineering project using Apache Kafka, Python, and Snowflake.

This project demonstrates how Kafka is actually used in real systems — including message keys, partitioning, streaming data validation, offset management, and warehouse-first analytics — not just basic producers and consumers.

It follows a Medallion Architecture (Bronze → Silver → Gold) approach, where:
Kafka handles real-time ingestion and data quality
Python handles stream processing
Snowflake handles analytics and business logic using SQL

## ⚡ Stack Used
- **Python**
- **ApacheKafka** 
- **Docker** 
- **Kafka producers and consumers**
- **Real-time stream processing**
- **Snowflake**

---

## ✅ Key Takeaways
The Learning Goals of this hands-on project:

• Generate continuous real-time events using Python
• Design Kafka topics with message keys and partitions
• Understand how Kafka partitioning and ordering actually work
• Build a Bronze → Silver streaming pipeline
• Clean and validate streaming data using Python consumers
• Manage Kafka offsets safely (no data loss)
• Load real-time data into Snowflake efficiently
• Separate streaming logic from analytics logic
• Implement Gold-layer transformations using Snowflake SQL

**Author:** *Peace KASSA*  