# 🧠 Dynamic Kafka Streaming System

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Kafka](https://img.shields.io/badge/Kafka-2.x+-orange.svg)](https://kafka.apache.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0+-blue.svg)](https://www.mysql.com/)
[![Flask](https://img.shields.io/badge/Flask-2.x+-green.svg)](https://flask.palletsprojects.com/)

A dynamic content streaming platform built with **Apache Kafka**, **Flask**, and **MySQL** that enables real-time data streaming with role-based access control.

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Features](#-features)
- [Tech Stack](#-tech-stack)
- [Database Schema](#-database-schema)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Usage](#-usage)
- [API Endpoints](#-api-endpoints)
- [Troubleshooting](#-troubleshooting)

## 🎯 Overview

This project demonstrates a multi-node streaming platform where:

- **Producers** request topics and stream data from multiple sources (Random, CSV, yFinance)
- **Admins** manage topic lifecycle (approve, activate, deactivate, reject)
- **Consumers** subscribe to approved topics and consume live data streams

All nodes communicate via a shared **MySQL database** and **Kafka broker**, enabling decentralized yet coordinated operations.

```
[ Producer Node(s) ] → [ Kafka Broker ] → [ Consumer Node(s) ]
         ↘              ↑              ↙
          ↘————→ MySQL (Topics, Logs, Subscriptions) ←————↙
```

## 🏗 Architecture

```
   ┌──────────────────────────┐
   │        Admin UI          │
   │  (admin_app.py :5003)    │
   │  • Manage Topics         │
   │  • Approve/Reject        │
   │  • Monitor Logs          │
   └──────────┬───────────────┘
              │
              ↓
     MySQL (topics, logs, subscriptions)
              │
              ↓
┌─────────────────────────────────────┐
│     Kafka Cluster / Broker          │
│     (e.g., 172.29.70.143:9092)      │
└─────────────────┬───────────────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
        ↓                   ↓
┌──────────────────┐  ┌──────────────────┐
│  Producer UI     │  │  Consumer UI     │
│ (producer.py)    │  │ (consumer_gui.py)│
│ :5001            │  │ :5002            │
│                  │  │                  │
│ • Request topics │  │ • Subscribe      │
│ • Stream data    │  │ • View streams   │
│ • Log to DB      │  │ • Store msgs     │
└──────────────────┘  └──────────────────┘
```

### Node Responsibilities

| Node | Script | Port | Description |
|------|--------|------|-------------|
| 🧑‍💼 **Admin** | `admin_app.py` | 5003 | Topic approval, lifecycle management, monitoring |
| 🧑‍🏭 **Producer** | `producer.py` | 5001 | Topic creation, data streaming (Random/CSV/yFinance) |
| 🧑‍💻 **Consumer** | `consumer.py` | 5002 | Topic subscription, message consumption, user management |
| ⚙️ **Broker** | Kafka Service | 9092 | Message routing, topic hosting |
| 🗄️ **Database** | MySQL | 3306 | Metadata storage, logs, subscriptions |

## ✨ Features

### Producer Features
- 📊 Multiple data sources (Random, CSV, yFinance)
- 🔄 Dynamic topic request and management
- 📈 Real-time streaming with configurable intervals
- 📝 Automatic logging to database

### Admin Features
- ✅ Topic approval/rejection workflow
- 🎛️ Topic activation/deactivation controls
- 📋 Comprehensive monitoring dashboard
- 👥 Subscription management

### Consumer Features
- 🔐 User-based authentication
- 📡 Dynamic topic subscription
- 💾 Message persistence (last 20 per topic/user)
- 🔄 Auto-refresh live feed

## 🛠 Tech Stack

- **Backend Framework:** Flask
- **Message Broker:** Apache Kafka
- **Database:** MySQL 8.0+
- **Data Processing:** Pandas, NumPy
- **Data Sources:** CSV, Random generators, yFinance API
- **Frontend:** Bootstrap 5

## 📊 Database Schema

The system uses a MySQL database named `kafka_stream` with the following schema:

```sql
-- Topics table
CREATE TABLE IF NOT EXISTS topics (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) UNIQUE NOT NULL,
  status ENUM('pending','approved','active') DEFAULT 'pending',
  INDEX idx_status (status)
);

-- User subscriptions
CREATE TABLE IF NOT EXISTS user_subscriptions (
  id INT AUTO_INCREMENT PRIMARY KEY,
  `user` VARCHAR(255) NOT NULL,
  topic_id INT NOT NULL,
  UNIQUE KEY unique_sub (`user`, topic_id),
  INDEX idx_user (`user`)
);

-- Producer logs
CREATE TABLE IF NOT EXISTS producer_logs (
  id INT AUTO_INCREMENT PRIMARY KEY,
  topic_name VARCHAR(255) NOT NULL,
  message JSON,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_topic_time (topic_name, timestamp)
);

-- Consumer logs
CREATE TABLE IF NOT EXISTS consumer_logs (
  id INT AUTO_INCREMENT PRIMARY KEY,
  username VARCHAR(255) NOT NULL,
  topic_name VARCHAR(255) NOT NULL,
  message JSON,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_user_topic_time (username, topic_name, timestamp)
);
```

### Topic Status Lifecycle

```
pending → (Admin Approves) → approved → (Producer Starts) → active
                                              ↓
                              (Admin Deactivates) → approved
                                              ↓
                              (Admin Rejects) → deleted
```

## 📦 Installation

### Prerequisites

- Python 3.9 or higher
- Apache Kafka + Zookeeper
- MySQL Server 8.0+
- Network connectivity between all nodes

### Step 1: Clone the Repository

```bash
git clone https://github.com/Shreya-CB/137_Project2_BD.git
```

### Step 2: Install Python Dependencies

```bash
pip install -r requirements.txt
```

**requirements.txt:**
```txt
flask>=2.0.0
kafka-python>=2.0.0
mysql-connector-python>=8.0.0
pandas>=1.3.0
yfinance>=0.2.0
numpy>=1.21.0
```

### Step 3: Set Up MySQL Database

```bash
mysql -u root -p

# In MySQL shell:
CREATE DATABASE kafka_stream;
USE kafka_stream;

# Run the SQL schema above
```

### Step 4: Configure Kafka

```bash
# Start Zookeeper
zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Broker
kafka-server-start.sh config/server.properties
```

**Important:** Set `auto.create.topics.enable=false` in `server.properties` to ensure topics are only created through the admin workflow.

## ⚙️ Configuration

Update the following configuration in each script (`admin_app.py`, `producer.py`, `consumer_gui.py`):

```python
# Database Configuration
DB_CONFIG = {
    "host": "<MYSQL_IP>",
    "user": "team",
    "password": "team137",
    "database": "kafka_stream"
}

# Kafka Configuration
KAFKA_BROKER = "<KAFKA_BROKER_IP>:9092"
```

### Port Configuration (Optional)

| Service | Default Port | Change In |
|---------|--------------|-----------|
| Admin UI | 5003 | `admin_app.py` |
| Producer UI | 5001 | `producer.py` |
| Consumer UI | 5002 | `consumer.py` |

## 🚀 Usage

### Starting the System

#### 1. Start Kafka Infrastructure

```bash
# Terminal 1: Start Zookeeper
zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2: Start Kafka Broker
kafka-server-start.sh config/server.properties
```

#### 2. Start Admin Node

```bash
python3 admin_app.py
# Access at: http://<ADMIN_IP>:5003
```

#### 3. Start Producer Node

```bash
python3 producer.py
# Access at: http://<PRODUCER_IP>:5001
```

#### 4. Start Consumer Node

```bash
python3 consumer_gui.py
# Access at: http://<CONSUMER_IP>:5002
```

### End-to-End Workflow

1. **Producer** requests a new topic
   - Topic created with `status='pending'`
   
2. **Admin** approves the topic
   - Status changes to `approved`
   
3. **Producer** starts streaming data
   - Status changes to `active`
   - Data flows to Kafka broker
   
4. **Consumer** subscribes to the topic
   - Begins receiving live messages
   - Messages stored in database
   
5. **Admin** can deactivate topic
   - Status reverts to `approved`
   - Producer automatically stops streaming

### Producer Modes

#### Random Data Mode
- Select columns (temperature, humidity, pressure, etc.)
- Configure streaming interval
- Generate synthetic data

#### CSV Upload Mode
- Upload CSV file
- Select columns to stream
- Configure streaming interval

#### yFinance Mode
- Enter stock ticker (e.g., `TCS.NS`, `AAPL`)
- Select OHLCV columns
- Stream real-time market data

## 📡 API Endpoints

### Admin API (`admin_app.py:5003`)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Admin dashboard |
| `/approve/<topic_id>` | POST | Approve a topic |
| `/reject/<topic_id>` | POST | Reject and delete a topic |
| `/deactivate/<topic_id>` | POST | Deactivate an active topic |
| `/topics` | GET | List all topics (JSON) |
| `/subscriptions` | GET | List all subscriptions (JSON) |

### Producer API (`producer.py:5001`)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Producer dashboard |
| `/request_topic` | POST | Request a new topic |
| `/upload_csv` | POST | Upload CSV file |
| `/start_topic` | POST | Start streaming a topic |
| `/stop_topic` | POST | Stop streaming a topic |
| `/status/<topic>` | GET | Get topic status (JSON) |

### Consumer API (`consumer_gui.py:5002`)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Consumer dashboard |
| `/login` | POST | User login |
| `/subscribe` | POST | Subscribe to a topic |
| `/unsubscribe` | POST | Unsubscribe from a topic |
| `/topics` | GET | List approved topics (JSON) |
| `/messages/<topic>` | GET | Get messages for a topic (JSON) |

## 🐛 Troubleshooting

### Common Issues

#### Connection Refused to Kafka

```bash
# Check if Kafka is running
kafka-topics.sh --list --bootstrap-server <KAFKA_BROKER_IP>:9092

# Check network connectivity
telnet <KAFKA_BROKER_IP> 9092
```

#### MySQL Connection Errors

```bash
# Verify MySQL is running
systemctl status mysql

# Test connection
mysql -h <MYSQL_IP> -u team -p
```

#### Topics Not Appearing

```bash
# Check database
mysql -u team -p kafka_stream
SELECT * FROM topics;

# Check Kafka topics
kafka-topics.sh --list --bootstrap-server <KAFKA_BROKER_IP>:9092
```

#### Producer Not Streaming

1. Verify topic status is `approved` or `active`
2. Check Admin has approved the topic
3. Verify Kafka broker connectivity
4. Check producer logs in database

### Useful Commands

```bash
# List all Kafka topics
kafka-topics.sh --list --bootstrap-server <KAFKA_BROKER_IP>:9092

# Describe a specific topic
kafka-topics.sh --describe --topic <TOPIC_NAME> --bootstrap-server <KAFKA_BROKER_IP>:9092

# Delete a Kafka topic
kafka-topics.sh --delete --topic <TOPIC_NAME> --bootstrap-server <KAFKA_BROKER_IP>:9092

# View producer logs
mysql -u team -p -e "SELECT * FROM kafka_stream.producer_logs ORDER BY id DESC LIMIT 20;"

# View consumer logs for a user
mysql -u team -p -e "SELECT * FROM kafka_stream.consumer_logs WHERE username='<USER>' ORDER BY id DESC LIMIT 20;"
```

## 🏆 Project Rubric Compliance

| Component | Implementation |
|-----------|----------------|
| **Producer** | ✅ Topic watcher, Publisher, Multi-mode input (Random/CSV/yFinance) |
| **Broker** | ✅ Kafka setup, Topic visibility, Reliable message routing |
| **Consumer** | ✅ Dynamic subscribe/unsubscribe, Accurate consumption, Thread-safe |
| **Admin/Client** | ✅ Monitoring UI, DB integration, Control panel |
| **E2E Demo** | ✅ 4 nodes on 4 systems, Graceful updates |

## 👥 Contributors

- **Team 137** - Big Data Project
- Shreya C
- Shrishti Bansal
- Sparsha M P
- Varshini K M


**Note:** For production deployment, ensure proper security measures including:
- Authentication and authorization
- Encrypted connections (SSL/TLS)
- Input validation and sanitization
- Rate limiting
- Proper error handling and logging
