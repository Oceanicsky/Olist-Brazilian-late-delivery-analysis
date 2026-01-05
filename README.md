# Late Delivery Analysis for Olist Brazilian dataset

## Introduction
The project use Brazilian e-commerce olist dataset to analyze late deliveries pattern in Power BI, and meanwhile realize end-to-end analysis from pulling data into SQL server to displaying the visuals to extract insights. Meanwhile, the analysis is dynamic and timely by enabling auto scheduled refreshed and auto cleaning the data, showing the most up-to-date visuals every day (though the dataset is static, deriving and cleaning the data are dynamic).   

## Tools
- Power BI
- SQL Server
- Python

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

## How to configure database and scheduled refresh
Prerequisite: 
- Local SQL server & a Database (named ecommerce_olist or modify the database name in db_config.py using your database name)
- On premise Gateway for Power BI (might updated to cloud database and omit the step). See for installation and configuring: https://learn.microsoft.com/en-us/data-integration/gateway/service-gateway-install

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

## Key Analysis
- Late rate and Average late days by review scores, sellers, customers, products, delivered date (customers receiving products) and other timestamp including purchase date, approved date, delivery date (sellers delivering products), Estimated Delivered date (Latest date for products delivered to customers), Shipping Limit (Latest date for sellers to deliver products), Review creation date and Review answer date.
- Drillthrough pages for factors, including divisions of time spending at each stages, comparisons with groups, orders details and summary. 
- comparisons with different timestamp for seasonal patterns 

## Possible improvements
- Cloud database
- Future rules on cleaning when data increases
- Alert in Power BI service after publishing 

