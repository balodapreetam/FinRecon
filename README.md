# 💳 Bank Transaction Reconciliation System

A real-time data pipeline to identify and store **mismatched bank transactions** from two sources—Gateway and Ledger—using **Apache Kafka**, **Apache Spark Structured Streaming**, and **Snowflake**.

---

## 🚀 Project Overview

This project simulates a financial reconciliation system where two Kafka producers emit bank transactions from `gateway` and `ledger`. Spark consumes, processes, and compares these streams, then writes any mismatches (e.g., missing in one source) into a Snowflake data warehouse for auditing and analytics.

---

## 🛠️ Tech Stack

| Layer              | Tools Used                               |
|--------------------|-------------------------------------------|
| **Programming**     | Python                                   |
| **Streaming Engine**| Apache Spark Structured Streaming        |
| **Message Broker**  | Apache Kafka                             |
| **Data Warehouse**  | Snowflake                                | |

---

## ⚙️ Key Features

- 🔄 **Kafka Producers** for Gateway and Ledger streams
- 🔍 **Full Outer Join** to detect mismatches
- 🧼 **Stream Cleaning** using `coalesce` and status flags
- ❄️ **Snowflake Sink** using `foreachBatch`
- 💾 **Checkpointing** for fault tolerance

---

## 📂 Project Structure

bash
├── gateway_producer.py       # Sends simulated Gateway transactions to Kafka
├── ledger_producer.py        # Sends simulated Ledger transactions to Kafka
├── consumer_spark.py         # Spark Structured Streaming job to process & write to Snowflake
└── README.md                 # Project documentation

___



