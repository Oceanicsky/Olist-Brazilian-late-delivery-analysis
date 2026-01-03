# Late Delivery Analysis for Olist Brazilian dataset

## Introduction
The project use Brazilian e-commerce olist dataset to analyze late deliveries pattern in Power BI, and meanwhile realize end-to-end analysis from pulling data into SQL server to displaying the visuals to extract insights. Meanwhile, the analysis is dynamic and timely by enabling auto scheduled refreshed and auto cleaning the data, showing the most up-to-date visuals every day (though the dataset is static, the deriving and cleaning are dynamic).   

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
- On premise Gateway

## Tools
- Power BI
- SQL Server
- Python

## Key Analysis
- Late rate by state and seller
- Drillthrough seller diagnostics
- Delivery vs shipping delay comparison

## Possible improvements
- cloud database
- Future rules on cleaning when data increases

## Result
