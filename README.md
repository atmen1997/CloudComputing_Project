# Distributed Word Counter on AWS

This project implements a **distributed word counting system** using Amazon Web Services (AWS). The architecture uses **S3**, **SQS**, and **EC2** to process large text datasets in parallel across multiple nodes.

---

## 🚀 Project Overview

A large text file is split into chunks, and each chunk is processed independently by an EC2 instance. The results are aggregated to produce the final word count. Two design variations were tested:

- **Design 1**: Shared process queue (no load balancing)
- **Design 2**: Dedicated queues per worker (with load balancing)

---

## 🧰 Stack & Tools

- **Language**: Python 3.9.6
- **Cloud**: AWS EC2, S3, SQS
- **Libraries**: `boto3`, `os`, `json`, `hashlib`, `time`
- **Architecture**: Master-worker pattern
- **Queue Type**: Standard & FIFO (with SHA-256 deduplication)

---

## 🧱 System Components

- **S3**: Stores the input dataset and outputs
- **SQS**: Manages distributed task queues
- **EC2**: Executes the word counting tasks
- **Master Node**: Splits data, distributes tasks, aggregates results
- **Worker Node**: Pulls tasks, processes chunks, returns results

---

## 🗃️ Design Comparison

### ✅ Design 1 – No Load Balancing
- One shared queue for all tasks
- Simple setup, but underutilizes EC2 nodes
- Slower due to uneven task distribution

### ✅ Design 2 – With Load Balancing
- One queue per EC2 node
- Dynamic round-robin assignment
- Significantly improved speed and scalability

---

## 📊 Performance Highlights

### Dataset: 48M+ words  
(Original Shakespeare text duplicated ×50)

### Best Configuration – Design 2:
- **EC2 nodes**: 1  
- **Chunk size**: 1,000,000 lines  
- **Queue size**: 10  
- **Processing time**: **0.5 seconds**

| Design     | EC2s | Chunk Size | Queue Size | Time (s) | Speedup |
|------------|------|------------|------------|----------|---------|
| Design 1   | 3    | 1.5M lines | 5          | 6.04     | 1×      |
| Design 2   | 1    | 1M lines   | 10         | 0.50     | 12×     |

> 🔍 Larger chunk sizes (1M–1.5M) reduced queue overhead.  
> 🧠 Load balancing (Design 2) led to up to **12× faster processing**.

---

## 💸 Cost Summary

- **Total AWS cost**: ~$9.30  
- EC2: $5.01 (most expensive)  
- S3: $2.83  
- SQS: $0.15 (minimal, mostly free-tier)

> Biggest spike: $3.61 on one day due to repeated S3 downloads during debugging.

---

## 🧪 Validation

All output word counts matched the expected baseline: **48,325,050 words**  
- Validated using an online tool and custom Python scripts  
- Error-free across all configurations
