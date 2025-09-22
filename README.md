# Real-time Order-Payment Stream Processing Pipeline

A robust real-time streaming data pipeline that processes order and payment events using Apache Kafka and Apache Spark, with stateful stream-stream joins and MongoDB storage.

## 🚀 Architecture Overview

This project implements a complete streaming architecture that handles order and payment events in real-time, performing stateful joins to correlate payments with their corresponding orders. The system is designed to handle duplicate events, out-of-order arrivals, and provides comprehensive logging and monitoring.

[4]

### Key Features
- **Real-time Stream Processing**: Processes order and payment events as they arrive
- **Stateful Stream-Stream Joins**: Correlates payments with orders using Spark's stateful processing
- **Duplicate Handling**: Intelligently handles duplicate events from both producers  
- **Fault Tolerance**: Includes checkpointing and state timeout mechanisms
- **Comprehensive Logging**: Detailed application logs stored in Google Cloud Storage
- **Scalable Architecture**: Built on Apache Spark for horizontal scalability

## 🛠 Technologies Used

| Technology | Purpose |
|------------|---------|
| ![Kafka Logo][10] **Apache Kafka** | For messaging queue |
| **Apache Spark** | Structured streaming processing engine |
| ![MongoDB Logo][6] **MongoDB** | Document database for joined results |
| ![Python Logo][10] **Python/PySpark** | Primary programming language |
| ![GCS Logo][5] **Google Cloud Storage** | Log storage and checkpointing |
| **Confluent Cloud** | Managed Kafka service |

## 📋 Prerequisites

- Python 3.8+
- Apache Spark 3.5+
- Access to Confluent Cloud (or Kafka cluster)
- MongoDB instance 
- Google Cloud Platform account (for GCS, Dataproc)
- Java 8+ (for Spark)

## ⚙️ Installation and Configuration

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/realtime-order-payment-streaming.git
cd realtime-order-payment-streaming
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. Environment Setup
Create a `.env` file in the project root:
```bash
cp .env.example .env
```

Update the `.env` file with your configurations:
```env
# Kafka Configuration (Confluent Cloud)
KAFKA_BOOTSTRAP=your-kafka-bootstrap-servers
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_USERNAME=your-kafka-username
KAFKA_SASL_PASSWORD=your-kafka-password

# Kafka Topics
KAFKA_ORDER_TOPIC=stream_order_producer
KAFKA_PAYMENT_TOPIC=stream_payment_producer
```

### 4. Kafka Topics Creation
Create the required topics in your Kafka cluster:
```bash
# Create order topic
kafka-topics --create --topic stream_order_producer --partitions 3 --replication-factor 1

# Create payment topic  
kafka-topics --create --topic stream_payment_producer --partitions 3 --replication-factor 1
```

### 5. MongoDB Setup
- Set up MongoDB instance (local or cloud)
- Update the MongoDB connection URI in `join_stream.py`
- Create database: `spark_streaming`
- Collection will be created automatically: `spark_streaming_stateful_writes`

### 6. Google Cloud Storage
- Create GCS bucket for logs and checkpoints
- Update bucket paths in `join_stream.py`
- Ensure proper authentication (service account or gcloud auth)

## 🚀 Usage

### Running the Complete Pipeline

1. **Start the Streaming Processor** (in terminal 1):
```bash
python join_stream.py
```

2. **Generate Order Events** (in terminal 2):
```bash
python order_producer.py
```

3. **Generate Payment Events** (in terminal 3):
```bash
python payment_producer.py
```

### Individual Component Usage

**Order Producer**: Generates sample order events
```bash
python order_producer.py
```

**Payment Producer**: Generates payment events for existing orders
```bash
python payment_producer.py
```

**Stream Processor**: Processes and joins the streams
```bash
python join_stream.py
```

### Monitoring

- **Application Logs**: Check local logs and GCS bucket
- **Spark UI**: Access at `http://localhost:4040` during processing
- **MongoDB**: Query the `spark_streaming_stateful_writes` collection for joined results

## 📁 Code Structure

```
├── order_producer.py          # Kafka producer for order events
├── payment_producer.py        # Kafka producer for payment events  
├── join_stream.py            # Spark streaming processor with stateful joins
├── requirements.txt          # Python dependencies
├── .env.example             # Environment variables template
└── README.md               # Project documentation
```

### Key Components

#### Order Producer (`order_producer.py`)
- Generates random order events with unique order IDs
- Sends events to `stream_order_producer` topic
- Includes random duplicate generation for testing
- Uses Confluent Kafka Python client

#### Payment Producer (`payment_producer.py`)  
- Generates payment events linked to order IDs
- Sends events to `stream_payment_producer` topic
- Simulates payment processing delays
- Compatible with order producer's ID scheme

#### Stream Processor (`join_stream.py`)
- **Spark Structured Streaming** application
- Reads from both Kafka topics simultaneously
- Implements **stateful stream-stream joins** using `applyInPandasWithState`
- Handles event deduplication and ordering issues
- Features:
  - State timeout management (15 minutes)
  - Comprehensive error handling and logging
  - MongoDB sink for joined results
  - GCS integration for log storage

### Data Schemas

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

**Joined Output Schema:**
```json
{
  "order_id": "string",
  "order_date": "ISO timestamp",
  "created_at": "ISO timestamp", 
  "customer_id": "string",
  "order_amount": "integer",
  "payment_id": "string",
  "payment_date": "ISO timestamp",
  "payment_amount": "integer"
}
```

## 🔧 Configuration Options

### Spark Configuration
Key Spark settings in `join_stream.py`:
- `spark.sql.shuffle.partitions`: Controls parallelism
- `spark.streaming.backpressure.enabled`: Enables adaptive query execution
- `spark.streaming.kafka.maxRatePerPartition`: Controls ingestion rate

### State Management
- **Timeout Duration**: 15 minutes (configurable)
- **Checkpoint Location**: GCS bucket for fault tolerance
- **State Structure**: Maintains order details until payment arrives

### Logging
- **Local Logs**: Timestamped files in specified directory
- **GCS Upload**: Automatic log upload after processing
- **Log Level**: INFO (configurable to DEBUG for troubleshooting)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit your changes: `git commit -m 'Add amazing feature'`
4. Push to the branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

### Development Guidelines
- Follow PEP 8 style guidelines
- Add comprehensive docstrings
- Include unit tests for new features
- Update documentation for any API changes

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Built with ❤️ using Apache Kafka, Apache Spark, and Python**
