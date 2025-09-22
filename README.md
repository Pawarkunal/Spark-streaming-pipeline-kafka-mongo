# Real-time Order-Payment Stream Processing on Google Cloud Dataproc

A robust real-time streaming data pipeline that processes order and payment events using Apache Kafka and Apache Spark on Google Cloud Dataproc, with stateful stream-stream joins and MongoDB storage.

## 🚀 Architecture Overview

This project implements a complete cloud-native streaming architecture that handles order and payment events in real-time on **Google Cloud Dataproc**, performing stateful joins to correlate payments with their corresponding orders. The system is designed to handle duplicate events, out-of-order arrivals, and provides comprehensive logging and monitoring with full GCS integration.

[16]

### Key Features
- **Cloud-Native Architecture**: Fully optimized for Google Cloud Dataproc deployment
- **Real-time Stream Processing**: Processes order and payment events as they arrive
- **Stateful Stream-Stream Joins**: Correlates payments with orders using Spark's stateful processing
- **Duplicate Handling**: Intelligently handles duplicate events from both producers  
- **Auto-scaling**: Leverages Dataproc's auto-scaling capabilities for cost optimization
- **GCS Integration**: Comprehensive logging and checkpointing to Google Cloud Storage
- **Production Ready**: Built for enterprise-scale deployment with monitoring and fault tolerance

## 🛠 Technologies Used

| Technology | Purpose | Deployment |
|------------|---------|-------------|
| **Apache Kafka** | Message streaming platform | Confluent Cloud |
| **Apache Spark** | Stream processing engine | Google Cloud Dataproc |
| **MongoDB** | Document database | MongoDB Atlas |
| **Python/PySpark** | Primary programming language | Dataproc runtime |
| **Google Cloud Storage** | Log storage and checkpointing | Native GCS integration |
| **Google Cloud Dataproc** | Managed Spark clusters | Auto-scaling clusters |


## 📋 Prerequisites

### Google Cloud Platform
- Google Cloud Project with billing enabled
- APIs enabled: Dataproc API, Compute Engine API, Cloud Storage API
- Service account with permissions:
  - **Dataproc Worker** (for cluster operations)
  - **Storage Object Admin** (for GCS access)
  - **Compute Instance Admin** (for cluster management)

### External Services
- **Confluent Cloud** account (or Kafka cluster access)
- **MongoDB** instance (Atlas or self-hosted)
- **gcloud CLI** installed and configured

### Local Development (Optional)
- Python 3.8+ (for local testing of producers)
- Java 8+ (for local Spark development)

## ⚙️ Installation and Configuration

### 1. Clone the Repository
```bash
git clone https://github.com/Pawarkunal/Spark-streaming-pipeline-kafka-mongo.git
```

### 2. Google Cloud Setup
```bash

# Create GCS bucket for code and logs
gsutil mkdir gs://kafka-mongo-stateful-streaming/stateful_streaming_logs/
gsutil mkdir gs://kafka-mongo-stateful-streaming/logging/
```

### 3. Environment Configuration
Create your `.env` file from the Dataproc template:
```bash
cp .env-dataproc.example .env
```

Update the `.env` file with your configurations:
```env

# Kafka Configuration (Confluent Cloud)
KAFKA_BOOTSTRAP=your-kafka-bootstrap-servers
KAFKA_SASL_USERNAME=your-kafka-username
KAFKA_SASL_PASSWORD=your-kafka-password

# MongoDB Connection
MONGODB_URI=mongodb+srv://username:password@cluster.mongodb.net/spark_streaming
```

## 📁 Code Structure

```
├── order_producer.py              # Kafka producer for order events
├── payment_producer.py            # Kafka producer for payment events  
├── join_stream.py                # Spark streaming processor with stateful joins
├── requirements-dataproc.txt      # Python dependencies for Dataproc
├── .env-dataproc.example         # Environment variables template for Dataproc
├── LICENSE                       # MIT License
└── README.md                     # Project documentation
```

### Key Components

#### Order Producer (`order_producer.py`)
- Generates random order events with unique order IDs
- Optimized for Dataproc job submission
- Uses Confluent Kafka Python client
- Can run locally or as Dataproc job

#### Payment Producer (`payment_producer.py`)  
- Generates payment events linked to order IDs
- Designed for concurrent execution with order producer
- Configurable via environment variables

#### Stream Processor (`join_stream.py`)
- **Production-grade Spark Structured Streaming** application
- Reads from both Kafka topics simultaneously on Dataproc
- Implements **stateful stream-stream joins** using `applyInPandasWithState`
- **Dataproc optimizations**:
  - Dynamic allocation enabled
  - GCS checkpointing
  - Auto-scaling support
  - Comprehensive logging to GCS

### Data Processing Flow

**Order Event Schema:**
```json
{
  "order_id": "string",
  "order_date": "ISO timestamp", 
  "created_at": "ISO timestamp",
  "customer_id": "string",
  "amount": "integer"
}
```

**Payment Event Schema:**
```json
{
  "payment_id": "string",
  "order_id": "string", 
  "payment_date": "ISO timestamp",
  "created_at": "ISO timestamp", 
  "amount": "integer"
}
```

**Joined Output (MongoDB):**
```json
{
  "order_id": "string",
  "order_date": "ISO timestamp",
  "customer_id": "string",
  "order_amount": "integer",
  "payment_id": "string",
  "payment_date": "ISO timestamp",
  "payment_amount": "integer",
  "processed_at": "ISO timestamp"
}
```

## ⚙️ Dataproc Configuration

### Cluster Specifications
- **Master**: n1-standard-4 (4 vCPUs, 15GB RAM)
- **Workers**: 3-6 n1-standard-4 instances (auto-scaling)
- **Preemptible workers**: Optional for cost optimization
- **Boot disk**: 50GB SSD for optimal performance

### Spark Optimizations
```properties
spark.executor.memory=4g
spark.executor.cores=2
spark.driver.memory=2g
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.maxExecutors=10
spark.sql.adaptive.enabled=true
spark.streaming.backpressure.enabled=true
```

### Cost Optimization
- **Preemptible instances** for non-critical workloads
- **Auto-scaling** based on workload
- **Auto-termination** after idle periods
- **Regional persistent disks** for cost-effective storage

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/dataproc-enhancement`
3. Test on Dataproc development cluster
4. Commit your changes: `git commit -m 'Add Dataproc optimization'`
5. Push to the branch: `git push origin feature/dataproc-enhancement`
6. Open a Pull Request

### Development Guidelines
- Test all changes on Dataproc clusters
- Follow PEP 8 style guidelines  
- Add comprehensive docstrings
- Update Dataproc configuration documentation
- Include resource usage estimates

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

---

<div align="center">

## 🤝 Connect With Me

**Data Engineer | Cloud Architect | Streaming Specialist**

*Passionate about building scalable real-time data pipelines and cloud-native architectures*

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/your-linkedin-username)
[![X (Twitter)](https://img.shields.io/badge/X-000000?style=for-the-badge&logo=x&logoColor=white)](https://twitter.com/your-twitter-handle)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/your-github-username)
[![Email](https://img.shields.io/badge/Email-D14836?style=for-the-badge&logo=gmail&logoColor=white)](mailto:your.email@example.com)

**Let's discuss data engineering, streaming architectures, and cloud technologies!**

---

**Built with ❤️ for Google Cloud Dataproc using Apache Kafka, Apache Spark, and Python**

</div>

