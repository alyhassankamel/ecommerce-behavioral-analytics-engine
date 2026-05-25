# E-Commerce Behavioral Analytics & Recommendation Engine

An end-to-end big data pipeline for e-commerce behavioral analytics and recommendation generation using Apache Spark and MongoDB. The system processes large-scale web interaction logs to build user affinity profiles, perform market basket analysis, and power targeted marketing campaigns.

## Overview

This project was developed for the Big Data Engineering course (Spring 2026) and demonstrates how distributed data processing can be used to extract actionable insights from large-scale e-commerce activity logs.

The pipeline analyzes user behavior such as:
- Product views
- Cart additions
- Purchases
- Clickstream activity

Using Apache Spark for distributed computation and MongoDB for scalable NoSQL storage, the system generates:
- User preference profiles
- Product co-occurrence relationships
- Cart abandonment targeting recommendations

---

# System Architecture

## Pipeline Layers

### Input Layer
Raw e-commerce interaction logs (`ecommerce_logs.csv`) containing:
- User activity
- Product interactions
- Session behavior
- Purchase events

### Processing Layer
Apache Spark distributed processing using:
- MapReduce paradigm
- Behavioral analytics
- Market basket analysis
- Affinity score computation

### Storage Layer
MongoDB collections optimized for:
- Real-time recommendation lookups
- Denormalized user profiles
- Fast read-heavy workloads

### Output Layer
Targeted marketing campaign generation:
- High Discount Campaigns
- Standard Reminder Campaigns

---

# Technologies Used

- Apache Spark
- PySpark
- MongoDB
- Big Data Processing
- MapReduce
- Data Engineering
- Recommendation Systems

---

# MongoDB Schema Design

## 1. user_profiles Collection

Stores user affinity data and preferred shopping categories.

### Example Document

```json
{
  "_id": "...",
  "user_id": "12345",
  "top_categories": [
    {
      "category": "Electronics",
      "score": 45
    },
    {
      "category": "Clothing",
      "score": 30
    }
  ]
}
```

---

## 2. item_cooccurrence Collection

Stores product relationships discovered through market basket analysis.

### Example Document

```json
{
  "_id": "...",
  "item_a": "PROD001",
  "item_b": "PROD002",
  "cooccurrence_count": 1250
}
```

---

# Why MongoDB?

MongoDB was selected because it provides:

- Fast read performance for recommendation queries
- Flexible schema evolution
- Embedded document structures for denormalized retrieval
- Horizontal scalability through sharding
- Native Spark connector integration

Compared to traditional SQL databases, MongoDB avoids expensive JOIN operations and enables efficient real-time recommendation lookups.

---

# Data Flow

## Phase 1
Spark processes raw logs to:
- Calculate user affinity scores
- Generate product co-occurrence pairs
- Perform behavioral analytics

## Phase 2
Processed analytics data is stored in MongoDB collections.

## Phase 3
Spark joins cart abandonment events with user profiles to:
- Segment customers
- Generate targeted marketing recommendations
- Flag high-value recovery opportunities

---

# Features

- Distributed processing of large-scale datasets (10GB+)
- User behavior profiling
- Recommendation-ready NoSQL schema
- Market basket analysis
- Cart abandonment targeting
- Real-time optimized data retrieval

---

# Future Improvements

- Real-time streaming with Apache Kafka
- Collaborative filtering recommendations
- Personalized ranking models
- Dashboard visualization layer
- Deployment on cloud infrastructure

---

# Project Structure

```bash
├── data/
│   └── ecommerce_logs.csv
├── spark_jobs/
│   ├── user_affinity.py
│   ├── market_basket_analysis.py
│   └── campaign_targeting.py
├── mongodb/
│   └── schema_design.md
├── outputs/
│   ├── user_profiles.json
│   └── item_cooccurrence.json
└── README.md
```

---

# Learning Outcomes

This project demonstrates practical experience with:
- Big Data Engineering
- Distributed Computing
- Data Pipeline Design
- NoSQL Database Modeling
- Recommendation Systems
- Behavioral Analytics

---
