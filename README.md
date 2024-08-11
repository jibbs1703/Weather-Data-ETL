# Weather-Gas-ETL-Pipeline

This repository contains a robust ETL (Extract, Transform, Load) pipeline designed to collect, process,
and store hourly weather and atmospheric gas data. The pipeline is built to handle large-scale data processing
and is engineered to run efficiently using cloud resources in a containerized environment, allowing for 
deployment regardless of operating system. This pipeline is one part of a 3-layer process, with the second and
third layers consisting of creating a SQL Database and displaying current and future forecasts on a user-friendly
dashboard. 

## Pipeline Features

Data Extraction: The pipeline gathers real-time weather and atmospheric gas data from multiple APIs. The data
is fetched at hourly intervals, ensuring the dataset remains up-to-date.

Data Transformation: Once extracted, the data undergoes a series of transformations to clean, preprocess, and
structure it for storage. This involves formatting timestamps, and converting units to ensure consistency.

Data Loading: The transformed data is loaded into a scalable and secure data storage solution on AWS. In the 
first phase (shown in this repository), the pipeline loads the data as .csv files to a landing storage area in
AWS S3. The arrival of the data in its S3 bucket triggers a Lambda function to copy the data in a structured form 
to AWS Redshift or RDS, where the data sits in SQL form (Second Phase).

Cloud Infrastructure: The entire pipeline is deployed on AWS, leveraging services such as S3, EC2, Lambda, RDS,
and Redshift. This ensures high availability, scalability, and cost-efficiency as the daily cost of running 
the pipeline is just under $1.25. 

Orchestration with Apache Airflow: The ETL process is orchestrated using Apache Airflow, which allows for easy
scheduling, monitoring, and management of the data pipeline. Airflow DAGs (Directed Acyclic Graphs) were used 
to define the tasks and their dependencies, ensuring a smooth and reliable data workflow between the Extraction, 
Transformation and Loading Tasks.

Containerization with Docker: The pipeline is fully containerized using Docker, ensuring consistency across 
different environments and simplifying the deployment process.

Alerting System: The pipeline also implements notifications for pipeline failures such as  anomalies in data 
collection, HTTP Responses and authentication issues that could be experienced using the cloud services. 

## Technology Stack

- AWS Cloud Services: S3, EC2, Lambda, RDS/Redshift
- Pipeline Orchestration: Apache Airflow
- Pipeline Containerization: Docker
- Programming Language: Python
- Data Processing: Pandas, NumPy

## Setup and Deployment
- Clone the Repository:
```
https://github.com/jibbs1703/Weather-Gas-ETL.git
cd Weather-Gas-ETL
```

- Configure Environment Variables:

  - Create a `.env` file with your AWS credentials and API keys required for data extraction <br/> 
  - Edit contents of the config.yaml file to suit the needs of your project


- Build and Run Docker Container After Configuring Dockerfile and docker-compose.yaml:
```
docker-compose up --build
```
- Access Apache Airflow Tasks UI:

When the container is built and running, Airflow can be accessed via http://localhost:8080. At this port, 
you can monitor the ETL pipeline's progress, manage tasks and DAGS on the airflow UI. 
