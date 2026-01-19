# Late Delivery Analysis for Olist Brazilian dataset

## Project Overview

This repository presents a statistical analysis of delivery performance using the public Olist Brazilian E-commerce dataset. The goal of the project is to understand what factors are associated with late deliveries and to quantify delivery performance across different operational groups such as sellers, states, and time periods.

This project demonstrates exploratory data analysis (EDA), statistical metric design, group comparisons, risk scoring, and data storytelling — skills central to a data analyst or statistician role.

## Business Problem & Motivation

In e-commerce operations, late deliveries can negatively impact customer satisfaction, seller reputation, and revenue. For logistics and operational decision-making, answering the following questions is critical:

- What proportion of orders are delivered late?

- Which sellers, regions, or time periods exhibit higher rates of late delivery?

- How to quantify and compare delivery performance statistically?

- What operational insights can guide performance improvement?

The answers help support data-driven decisions for sellers, marketplace operators, and logistics planners.

## Dataset Description

The dataset is sourced from the public Olist Brazilian E-commerce dataset, which contains records of ~100k orders from 2016–2018, including order timestamps, delivery dates, seller and customer location information.

Main Data Tables: 

- Orders: order IDs, purchase and delivery dates

- Sellers / Customers: geographic locations

- Delivery Performance: actual vs estimated delivery durations

(The repository includes data cleaning and schema explanation.)


## How to configure database and scheduled refresh
Prerequisite: 
- Local SQL server & a Database (named ecommerce_olist or modify the database name in db_config.py using your database name)
- On premise Gateway for Power BI (might updated to cloud database and omit the step). No need for this step if using mannual refresh. See for installation and configuring: https://learn.microsoft.com/en-us/data-integration/gateway/service-gateway-install

Configure Kaggle API:
- Open Environment variables
- Click New and enter KAGGLE_API_TOKEN = KGAT_317e0fc5d2ccb5fa4bf9c903d961d7d
<img width="1160" height="559" alt="e6eb1d35-2b9d-4389-a0c2-af4350bf664a" src="https://github.com/user-attachments/assets/921fcf9a-fcda-4d2b-bf0b-5d84c511f978" />


Intial running: 
- Open scheduled refresh.bat
- If the program takes a long time to find anaconda, add anaconda path mannually in the bat file in the following part.
<img width="385" height="120" alt="3661e1cd-90d9-49c0-940f-8fb5d6c25008" src="https://github.com/user-attachments/assets/e334a453-d46b-4be8-ad7b-6651e4465678" />


Scheduled refreshed:
- Search and Open Windows Task Scheduler
- Create task and Name the task
- Click Triggers and Create a new trigger
- Configure the preferred frequency for scheduled refreshed (e.g. Daily), and Click OK
<img width="1335" height="858" alt="62085201-794a-400a-9128-6931beda1c42" src="https://github.com/user-attachments/assets/9bc53519-b739-4b55-9f07-126e709a56c3" />
- Click Actions and Create a new action
- Select "Start a program" action and add path of the scheduled refresh.bat file
- Click OK
<img width="1164" height="846" alt="a8f031e4-2ae5-46b1-b844-49df9f27d2d8" src="https://github.com/user-attachments/assets/510e7430-e1cf-40df-ac35-672293736284" />
- Click OK to save the task

## Analytical Approach

### 1. Data Preparation

- Merge relevant Olist tables
- Convert timestamp fields to datetime format
- Engineer key features:
  - `actual_delivery_days`
  - `estimated_delivery_days`
  - `late_delivery_flag` (actual > estimated)

---

### 2. Descriptive Statistics

- Overall late delivery rate
- Distribution of delivery durations
- Summary statistics (mean, median, percentiles)

---

### 3. Grouped Analysis

Delivery performance is compared across multiple dimensions:

- **Seller-level late delivery rate**
- **State-level late delivery rate**
- **Time-based trends** (monthly / quarterly)
- **Delivery duration brackets**

Group comparisons highlight operational heterogeneity and potential outliers.

---

### 4. Risk Stratification

- Delivery time percentiles are used to classify orders into risk groups
- Sellers and regions are ranked by late delivery risk
- This enables prioritization of operational interventions

---

### 5. Visualization

The notebook includes visualizations such as:

- Histogram of delivery times
- Late vs on-time delivery comparisons
- Bar charts of late delivery rate by seller and state
- Time-series trends of late delivery rates

---

## 📊 Key Insights

- A non-trivial proportion of orders are delivered later than estimated
- Longer delivery durations are associated with higher late delivery probability
- Significant variation exists across sellers and geographic regions
- Time-based patterns suggest potential seasonality and operational bottlenecks

## Possible improvements
- Cloud database
- Future rules on cleaning when data increases
- Alert in Power BI service after publishing 
- Use Weighted Avg Score in Risk Score
