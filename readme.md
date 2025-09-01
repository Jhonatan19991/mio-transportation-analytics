# MIO GPS Data Pipeline

A comprehensive data engineering project that extracts, processes, and analyzes GPS data from MIO (Massive Integrated Transportation System) in Cali, Colombia. This project combines Azure Functions for data extraction with Databricks Delta Live Tables (DLT) for automated data processing and analytics.

## 🚀 Project Overview

This project creates an end-to-end data pipeline that:
- **Extracts** real-time GPS data from MIO buses using Azure Functions
- **Processes** the data through a multi-layered DLT pipeline in Databricks
- **Delivers** clean, structured data for transportation analytics
- **Publishes** processed datasets to Kaggle for public research use

## 🏗️ Architecture

### Data Extraction (Azure Functions)
- **Location**: `Azure Function/function_app.py`
- **Purpose**: Scrapes and extracts GPS data from MIO transportation system
- **Data Sources**: 
  - MIO bus routes and stops
  - Real-time GPS coordinates
  - Bus metadata (route, orientation, delays)

### Data Processing (Databricks DLT)
- **Location**: `dlt/transformations/dlt_pipeline_gps.py`
- **Purpose**: Multi-stage data transformation pipeline
- **Pipeline Stages**:
  - **Bronze**: Raw data ingestion and initial parsing
  - **Silver**: Data cleaning, normalization, and enrichment
  - **Gold**: Business-ready analytics tables

### Data Exploration & Analysis
- **Location**: `dlt/explorations/notebooks/EDA.ipynb`
- **Purpose**: Exploratory data analysis and insights generation
- **Output**: Statistical analysis, visualizations, and data quality reports

## 📊 Data Pipeline Flow

```
MIO GPS Data → Azure Function → Databricks DLT → Gold Tables → Kaggle
     ↓              ↓              ↓              ↓          ↓
  Real-time    Data Scraping   ETL Pipeline   Analytics   Public Dataset
  GPS Data     & Extraction    (Bronze→Gold)   Tables      (Monthly Updates)
```

## 🎯 Key Features

### Automated Data Processing
- **Schedule**: Pipeline runs daily at 1:00 AM
- **Incremental Processing**: Only processes new data
- **Data Quality**: Built-in expectations and validation rules
- **Error Handling**: Robust error handling and logging

### Data Quality & Validation
- GPS coordinate validation and normalization
- Trip duration and velocity sanity checks
- Route and stop validation
- Missing data handling and imputation

### Analytics-Ready Outputs
- **Trip Analysis**: Duration, delays, velocity patterns
- **Route Performance**: Segment speeds and efficiency metrics
- **Time-based Insights**: Rush hour analysis, time-of-day patterns
- **Geographic Analysis**: Stop-to-stop performance metrics

## 📈 Available Datasets

### Gold Tables (Analytics Ready)
1. **`gold_trips_final`**: Complete trip analysis with:
   - Trip duration and delays
   - Average velocity and orientation
   - Time-of-day and rush hour classification
   - Route and stop information

2. **`gold_route_segment_speed`**: Route performance metrics:
   - Segment-by-segment speed analysis
   - Distance and observation counts
   - Median and average speeds per route segment

### Public Dataset on Kaggle
- **Dataset**: MIO GPS Transportation Data
- **Update Frequency**: Monthly
- **Access**: Open to public for research and analysis
- **Format**: Clean, structured CSV with 8 key columns
- **Link**: https://www.kaggle.com/datasets/jhonatanmorales/mio-dataset

## 🛠️ Technology Stack

- **Data Extraction**: Azure Functions, Python, BeautifulSoup
- **Data Processing**: Databricks, Delta Live Tables, PySpark
- **Data Storage**: Delta Lake, Azure Blob Storage
- **Data Analysis**: Jupyter Notebooks, Pandas, Matplotlib
- **Scheduling**: Databricks Jobs
- **Data Quality**: Great Expectations integration

## 🚦 Getting Started

### Prerequisites
- Azure subscription with Functions
- Databricks workspace
- Python 3.8+
- Required Python packages (see requirements.txt)

### Setup Instructions
1. **Azure Function Setup**:
   - Deploy the function app to Azure
   - Configure connection strings and environment variables
   - Set up appropriate triggers

2. **Databricks Setup**:
   - Import the DLT pipeline code
   - Configure cluster and storage settings
   - Set up job scheduling (daily at 1:00 AM)

3. **Data Access**:
   - Access processed data through Databricks tables
   - Download public dataset from Kaggle
   - Use exploration notebooks for analysis

## 📋 Project Structure

```
MIO_git/
├── Azure Function/
│   └── function_app.py          # GPS data extraction
├── dlt/
│   ├── explorations/
│   │   ├── data/               # Raw data files
│   │   └── notebooks/          # Analysis notebooks
│   ├── transformations/
│   │   └── dlt_pipeline_gps.py # Main DLT pipeline
│   ├── unit_tests/             # Pipeline testing
│   └── utilities/              # Helper functions
├── git_config.md               # Git configuration
└── README.md                   # This file
```

## 🔄 Pipeline Schedule

- **Frequency**: Daily at 1:00 AM (Colombia time)
- **Processing**: Incremental updates with new GPS data
- **Output**: Updated gold tables and analytics
- **Public Updates**: Monthly Kaggle dataset refresh

## 📊 Use Cases

- **Transportation Planning**: Route optimization and capacity planning
- **Performance Analysis**: Bus efficiency and delay analysis
- **Urban Mobility Research**: Academic and research applications
- **Public Transportation Insights**: Government and policy analysis
- **Data Science Projects**: Machine learning and predictive modeling

## 🤝 Contributing

This project is designed for public transportation research and improvement. Contributions are welcome for:
- Data quality improvements
- Additional analytics features
- Performance optimizations
- Documentation enhancements

## 📄 License

This project is open source and available for research and educational purposes. Please ensure proper attribution when using the data or code.

## 📞 Contact

For questions about the data pipeline or collaboration opportunities, please refer to the project documentation or create an issue in the repository.

---

**Note**: This project processes real-time transportation data and is designed to provide insights for improving public transportation services in Cali, Colombia.
