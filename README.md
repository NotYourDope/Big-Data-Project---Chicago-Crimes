# Chicago Crimes Data Analysis and Prediction

## Overview
This project is focused on analyzing crime data from the city of Chicago and developing machine learning models to predict whether a crime leads to an arrest. The project is built using **Apache Spark** for distributed data processing and **Apache Kafka** for near real-time data streaming.

The workflow consists of three main stages:

1.  **Data Inspection and Cleaning**
    -   Loading and understanding the dataset
    -   Handling missing values, duplicates, and categorical data
    -   Feature engineering and transformation for further analysis
    -   Exploratory Data Analysis (EDA) with statistical summaries and visualization

2.  **Machine Learning Model Development**
    -   Dataset preparation with feature engineering
    -   Addressing class imbalance using class weighting (as 77% of cases are non-arrests)
    -   Building three models:
        -   Linear Support Vector Machine (SVM)
        -   Logistic Regression
        -   Random Forest Classifier

3.  **Model Evaluation and Prediction**
    -   Evaluating model performance using appropriate metrics
    -   Comparing the performance of the models
    -   Discussing results and selecting the best approach

4. **Real-Time Crime Classification with Kafka + Spark**
	-   Integration with **Apache Kafka** to simulate near real-time crime data ingestion
	-   The trained model is applied to classify incoming crime records on-the-fly
	-   Prediction of whether the crime is likely to result in an arrest
	-   Demonstrates how batch-trained models can be deployed for streaming data use cases

# Tech Stack

-   **Apache Spark** (PySpark)
	> **Note:** Docker container has been used to run Spark environment
	
-   **Apache Kafka** (for real-time data streaming)

-   **Python** (Pandas, Matplotlib, Seaborn)

-   **Machine Learning Algorithms:**
    -   Logistic Regression
    -   Linear SVM
    -   Random Forest
  
# Dataset

The dataset has been downloaded from open source.
> **Source:** https://huggingface.co/datasets/gymprathap/Chicago-Crime-Dataset/resolve/main/Crimes_-_2001_to_Present_20240713.csv

# Project Report
Comprehensive project report can be acessed from the pdf file in the root folder of the project.
