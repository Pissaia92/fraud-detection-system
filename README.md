````markdown
# 🛡️ Real-Time Financial Fraud Detection System

> **Architecture:** Event-Driven (Streaming) | **Stack:** Python, InfluxDB, Grafana, Docker.

## 📋 Project Overview
This project simulates a **High-Frequency Trading (HFT)** environment to detect financial fraud in real-time.
Unlike traditional Batch processing (D+1), this architecture reduces detection latency from **24 hours to sub-second**.

It ingests streaming transaction data, applies a **Risk Engine** based on moving windows, and visualizes
financial impact immediately on a Command Center Dashboard.

## 🚀 Key Features
* **Event-Driven Architecture:** Decoupled ingestion and visualization using Time-Series principles.
* **Infrastructure as Code (IaC):** Fully containerized environment via `docker-compose`.
* **Observability:** Custom Grafana dashboard with **KPIs** (Total Loss), **Gauges** (Risk Level), and **Live Logs**.
* **Performance:** Optimized Flux queries using `pivot()` and `group()` for efficient aggregation of high-cardinality data.

## 🛠️ Tech Stack
* **Ingestion:** Python (Faker Library for synthetic financial data).
* **Storage:** InfluxDB 2.x (Time-Series Database optimized for high write throughput).
* **Visualization:** Grafana (Real-time alerting and monitoring).
* **Containerization:** Docker & Docker Compose.

## 📊 Dashboard Preview
![Dashboard Preview] (https://raw.githubusercontent.com/Pissaia92/fraud-monitor/refs/heads/main/src/assets/grafana-v2.gif)

## ⚡ How to Run

**Pre requisites:** Docker installed.

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Pissaia92/fraud-monitor.git
   ```

2.  **Start the cluster:**

    ```bash
    cd fraud-monitor
    docker-compose up -d --build
    ```

3.  **Access the Dashboard:**

      * Go to: [http://localhost:3000](https://www.google.com/search?q=http://localhost:3000)
      * **Login:** `admin`
      * **Password:** `admin`
