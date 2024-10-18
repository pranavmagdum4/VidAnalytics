# VidAnalytics
# Data Engineering YouTube Analysis Project

## Project Overview
The **Data Engineering YouTube Analysis Project** is designed to securely manage, streamline, and analyze structured and semi-structured data derived from YouTube videos. By focusing on video categories and trending metrics, this project aims to provide valuable insights into video performance and audience engagement.

## Project Objectives
- **Data Ingestion:** Develop a robust mechanism for ingesting data from multiple sources to ensure comprehensive analysis.
- **ETL System:** Implement an Extract, Transform, Load (ETL) system to convert raw data into a structured format suitable for analysis.
- **Data Lake:** Create a centralized repository for storing data from various sources, facilitating efficient access and management.
- **Scalability:** Ensure the system can handle increasing data volumes without performance degradation.
- **Cloud Infrastructure:** Utilize cloud services, specifically AWS, to manage and process large datasets effectively.
- **Reporting:** Design and build an interactive dashboard to visualize and derive insights from the data.

## Services Utilized
- **Amazon S3:** A scalable object storage service that ensures high availability and security for the stored data.
- **AWS IAM:** Identity and access management service for securely controlling access to AWS resources.
- **Amazon QuickSight:** A serverless business intelligence service that enables quick and easy data visualization and reporting.
- **AWS Glue:** A serverless data integration service that simplifies the discovery and preparation of data for analytics and machine learning.
- **AWS Lambda:** A serverless computing service that allows code execution without the need to manage server infrastructure.
- **AWS Athena:** An interactive query service that allows SQL queries directly on data stored in S3 without requiring prior loading.

## Dataset
The dataset used for this project is sourced from Kaggle, containing statistics on trending YouTube videos over an extended period. It includes details on up to 200 popular videos published daily across various regions, encompassing fields such as video title, channel title, publication time, tags, views, likes, dislikes, description, and comment count. Each region has its dedicated file, and a unique category_id is included for categorization.

- Dataset Link: [Kaggle YouTube Dataset](https://www.kaggle.com/datasets/datasnaek/youtube-new)

## Architecture Diagram
![Architecture Diagram](architecture.jpeg)

## Conclusion
This project not only aims to analyze YouTube video data effectively but also demonstrates the capabilities of cloud technologies in handling large datasets and deriving meaningful insights. By leveraging the power of AWS, we can ensure scalability, security, and efficiency in our data engineering practices.
