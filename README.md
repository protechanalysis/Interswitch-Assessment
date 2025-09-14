# Interswitch-Assessment

# Project File Explanations

This document explains the purpose of each file and folder in the project.

---

## Question 2 Directory

- **docker-compose.yaml** → Defines services for running Airflow, database(s), and other components using Docker Compose.  
- **dockerfile** → Instructions to build the custom Docker image with required dependencies.  
- **requirement.txt** → List of Python dependencies needed for the project.  
### dags/

Contains all Airflow DAG-related scripts.

- **includes/clickhouse.py**: Helper functions for connecting to and interacting with a ClickHouse database.  
- **notification/email_alert.py**: Sends email alerts (e.g., when a task fails in Airflow).  
- **sql_script/create_table.sql**: SQL script to create required database tables.  
- **sql_script/task1.sql**: SQL query for Task 1.  
- **task2.py**: Python script for Task 2 (ETL or custom data logic).  

### etl/

- **trip_metrics.db** → SQLite database file containing trip metrics data for testing SQL queries.  

### images/

Contains visual assets and diagrams for documentation.

- **assessment_airflow.png** → Diagram or screenshot of the overall Airflow DAG.  
- **dag_run.png** → Example screenshot of an Airflow DAG run.  
- **failure_alert.png** → Screenshot of failure alert notification.  
- **trip_metrics_data.png** → Visualization of trip metrics dataset.  

## Root SQL & Python Solutions

- **question_1.sql**: SQL solution for Question 1.  
- **question_3.sql**: SQL solution for Question 3.  
- **question_4.sql**: SQL solution for Question 4.  
- **question5.py**: Python solution for Question 5.  



## Tech Stack Used

- **Python 3.8+**
- **Apache Airflow** for orchestration
- **SQLite / ClickHouse** for data storage & queries
- **Docker & Docker Compose** for containerized environment
- **SMTP/Email** for task failure alerts


## How to setup using

### Clone the Repository
```bash
git clone https://github.com/protechanalysis/Interswitch-Assessment.git

## cd into airflow directory
cd question2
```

### Build and Run with Docker
```bash
docker-compose build

docker-compose up -d
```
