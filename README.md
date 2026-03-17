🏡 Airbnb Automated Data Pipeline & Analytics Dashboard

An end-to-end automated data pipeline project that processes Airbnb listings data, performs data cleaning and feature engineering, and generates analytics-ready datasets used in a Power BI dashboard.

The pipeline is automated using n8n workflow automation and executed inside a Docker container, ensuring reproducibility and automation.

---

📌 PROJECT HIGHLIGHTS

* Automated Python ETL pipeline
* Feature engineering for analytics
* Docker containerization
* n8n workflow automation
* Power BI dashboard
* SQL validation queries
* Exploratory data analysis (EDA)

---

⚙️ TECHNOLOGIES USED

* Python
* Pandas
* NumPy
* Docker
* n8n
* Power BI
* SQL
* Matplotlib
* Seaborn

---

## 🏗️ Project Architecture

- Raw Dataset  
  ↓  
- Python ETL Pipeline (clean_airbnb.py)  
  ↓  
- Data Cleaning & Feature Engineering  
  ↓  
- Processed Dataset  
  ↓  
- Power BI Dashboard  

---

## ⚙️ Workflow Automation

**Manual / Scheduled Trigger → n8n Workflow → Docker Container → Execute Python Pipeline → Generate Processed Dataset**

---

🚀 KEY FEATURES

Data Cleaning:

* Standardized column names
* Duplicate removal
* Data type conversion

Missing Value Handling:

* Median filling for numeric columns
* Logical defaults for categorical data

Outlier Detection:

* Price outliers handled using IQR method

Feature Engineering:

* Price per guest
* Price per bedroom
* Host experience years
* Host type (professional / individual)
* Luxury listing flag
* Overall review score
* Long stay flag

---

📂 PROJECT STRUCTURE

data/
raw/ → dataset from Kaggle
processed/ → cleaned output

pipeline/ → clean_airbnb.py

notebooks/ → EDA analysis

sql/ → validation queries

powerbi/ → dashboard screenshots

n8n_workflows/ → automation workflow

docs/ → project documentation

---

📊 POWER BI DASHBOARD

Dashboard includes:

* Price distribution analysis
* Host performance insights
* Review score trends
* Luxury vs standard listings
* Property type analysis

Power BI file download link:
([Google Drive link here](https://drive.google.com/file/d/146VO2D89fnEPf-PCdTnamn2QVlUdRu3_/view?usp=sharing))

## 📊 Dashboard Overview
![Overview Dashboard](Airbnb%20Automated%20Pipline%20Project/Airbnb_Automated_Pipeline_Project/powerbi/Screenshots/Overview.PNG)
---

📈 EXPLORATORY DATA ANALYSIS

EDA was performed using:

* Matplotlib
* Seaborn

Notebooks are available in the notebooks folder.

---

🗄 DATASET SOURCE

Kaggle Dataset:
https://www.kaggle.com/datasets/arianazmoudeh/airbnbopendata

Note: Dataset is not included due to GitHub size limitations.

After downloading, place it inside:
data/raw/

---

▶ RUNNING THE PIPELINE

Command:

cd /files && python pipeline/clean_airbnb.py

This will:

* Load dataset
* Clean and transform data
* Generate features
* Save processed dataset

Output stored in:
data/processed/

---

🎯 PROJECT GOAL

To build a complete automated data pipeline that:

* Processes raw data
* Ensures data quality
* Generates analytics-ready datasets
* Supports dashboard reporting

---

👨‍💻 AUTHOR

**Sailesh Kumar** - 
Data Analytics Portfolio Project

