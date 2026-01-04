# Late Delivery Analysis for Olist Brazilian dataset

## Introduction
The project use Brazilian e-commerce olist dataset to analyze late deliveries pattern in Power BI, and meanwhile realize end-to-end analysis from pulling data into SQL server to displaying the visuals to extract insights. Meanwhile, the analysis is dynamic and timely by enabling auto scheduled refreshed and auto cleaning the data, showing the most up-to-date visuals every day (though the dataset is static, deriving and cleaning the data are dynamic).   

## Business Problem
Late deliveries are a critical operational issue in e-commerce, directly impacting customer satisfaction, seller reputation, and repeat purchase behavior.

Using the Olist Brazilian e-commerce dataset, this project aims to identify:
- Where late deliveries occur most frequently (by state and seller)
- Which stage of the order lifecycle contributes most to delays (shipping vs final delivery)
- Whether late deliveries are driven by systemic regional issues or by seller-level performance differences

The goal is to provide actionable insights that can help:
- Operations teams prioritize process improvements
- Marketplace managers identify high-risk sellers
- Logistics teams focus on the most delay-prone stages of fulfillment

## How to use the files
Prerequisite: 
- Local SQL server & a Database (named ecommerce_olist or modify the database name in db_config.py using your database name)
- On premise Gateway for Power BI (might updated to cloud database and omit the step)

Intial running: 
- Open scheduled refresh.bat
- If the program takes a long time to find anaconda, add anaconda path mannually in the bat file in the following part.
<img width="385" height="120" alt="3661e1cd-90d9-49c0-940f-8fb5d6c25008" src="https://github.com/user-attachments/assets/e334a453-d46b-4be8-ad7b-6651e4465678" />


## Tools
- Power BI
- SQL Server
- Python

## Key Analysis
- Late rate by state and seller
- Drillthrough seller diagnostics
- Delivery vs shipping delay comparison

## Possible improvements
- Cloud database
- Future rules on cleaning when data increases

## Result
