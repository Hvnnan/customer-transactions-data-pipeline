# Sample Dataset

## Overview

This project uses a publicly available synthetic banking transactions dataset from Kaggle for demonstration purposes.

The dataset simulates real-world banking transactions and includes customer demographics, transaction details, device information, channel metadata, and security-related attributes. It is suitable for building end-to-end data engineering pipelines, exploratory data analysis, fraud detection, and financial analytics.

## Dataset Source

**Title**

Bank Transactions Dataset for Fraud Detection

**Author**

Thuan Dao

**Platform**

Kaggle

**URL**

https://www.kaggle.com/datasets/thuandao/bank-transactions-dataset-for-fraud-detection

## Dataset Used

The original dataset contains **50,000 synthetic banking transaction records** representing various customer transactions across multiple banking channels. The data includes transaction information, customer demographics, device metadata, and behavioral attributes designed for analytics and fraud detection use cases.

## Repository Note

The original dataset is **not included** in this repository.

To run this project:

1. Download the dataset from Kaggle.
2. Upload the CSV file to the Databricks Landing Volume:
   /Volumes/workspace/landing/raw_files/
3. Execute the Databricks Workflow or run the notebooks sequentially.

Expected input file:

bank_transactions_data_2_augmented_clean_2.csv
