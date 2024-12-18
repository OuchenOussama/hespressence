# Hespressence - Kappa Architecture Based Sentiment Analysis System

## Overview

**Hespressence** is a system designed to collect and analyze user-generated comments from **Hespress.com**. The system enables both exploratory analysis and real-time sentiment trend detection through a unified stream processing architecture. By adopting the **Kappa Architecture**, it treats all data as streams, simplifying the integration of batch and stream processing for enhanced scalability and consistency.

The system uses Apache Kafka for data ingestion, Apache Flink for stream processing, MongoDB for historical data storage, and PostgreSQL for analysis results and real-time processing. The visualization layer is powered by D3.js, providing an interactive dashboard for sentiment analysis and trend monitoring.

## Architecture

Below is a high-level overview of the **Hespressence** system architecture:

![System Architecture](architecture_diagram.png)


## Features

- **Data Ingestion**: Kafka is used to ingest comment events in real-time with partitioning for maintaining article-wise comment ordering.
- **Stream and Batch Processing**:
  - **Stream Processing**: Real-time sentiment analysis using Apache Flink's DataStream API.
  - **Batch Processing**: Historical comment analysis using MongoDB for raw data storage and Flink's Table API for processing.
- **Storage**: PostgreSQL serves as a unified database for both real-time and historical analysis, while MongoDB holds raw event data.
- **Sentiment Analysis**: Sentiment scores and trends are computed, supporting real-time and historical sentiment analysis.
- **Visualization**: A custom dashboard powered by D3.js for interactive sentiment visualizations and operational monitoring.

## Requirements

- Python 3.x
- Apache Kafka
- Apache Flink
- MongoDB
- PostgreSQL
- Docker
- Docker Compose
- Additional Python packages listed in `requirements.txt`

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/OuchenOussama/hespressence.git
cd hespressence
```

### 2. Docker Setup

To run the necessary services (Kafka, Zookeeper, MongoDB, PostgreSQL) via Docker Compose, run:

```bash
docker compose up -d --build
```

This will start all the services required for the application and will run the comment scraping process 

The data processing service (real-time and historical processing) are started through (automatically after build):

```bash
src/main.py
```

### 3. Access the Dashboard

Once the system is running, the dashboard for real-time sentiment monitoring can be accessed at the provided address in the browser (configured within the app).

## Configuration

The application's settings are managed through `src/config/settings.py` :

- **Kafka settings**: Topic names, broker addresses, and partition strategies.
- **Flink settings**: Flink cluster configuration, job execution settings.
- **Database settings**: PostgreSQL and MongoDB connection parameters.
- **Scraper settings**: Configuration for scraping and merging comment data.

## Usage

- **Scraping**: Scrape comments from the website using the `scraper_rss.py` script, which collects data and sends it to Kafka for processing.
- **Processing**: Real-time sentiment analysis is performed using Flink, and the results are sent to PostgreSQL.
- **Exploratory Analytics**: The dashboard allows for deep analysis of sentiment across different topics and timeframes.
- **Monitoring**: Track trends and monitor sentiment patterns for ongoing conversations through the dashboard.