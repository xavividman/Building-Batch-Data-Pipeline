# Building Batch Data Pipeline
## Author: Xavier Vidman

## Overview
The "Building-Batch-Data-Pipeline" repository is designed to facilitate a professional ETL (Extract, Transform, Load) process using Python, allowing for automation and execution through Apache Airflow. The primary data source for this project is the YFinance API from Yahoo, focusing on extracting hourly stock market data for the companies Apple, Amazon, Google, and Microsoft.

## Project Structure
The repository is organized into the following folders:

### 1. scripts
Contains essential scripts for initializing and loading data into the project's tables:

- `create_tables.py`: Creates the required tables for the project.
- `initial_load.py`: Performs the initial data loading into the tables.
### 2. notebooks
Includes helpful Jupyter notebooks for better understanding and visualization of the code:

- `01_data_extraction.ipynb`: Demonstrates the data extraction process from the YFinance API.
- `02_test_db_connection.ipynb`: Allows users to test the functionality of their database connection.

### 3. deploy_airflow
This folder facilitates the deployment of Airflow locally. It includes:

- `docker-compose.yaml`: File for deploying Airflow in a manner similar to the database.
- `Dockerfile`: custom image used by the docker-compose.yaml file.
- `common`: Holds the incremental_etl.py script, which contains the code for performing incremental ETL.
- `dags`: Contains Airflow DAGs for execution.
- `logs`: Placeholder directory for Airflow logs.
- `plugins`: Placeholder directory for Airflow plugins.
- `secrets`: Holds credential files.
- `config`: Placeholder directory for configuration files.
The repository also provides a requirements.txt file listing all dependencies required for the project.

To start the deployment, navigate to this directory in the terminal and run:

```bash
docker-compose up -d
```
To stop the database when no longer needed, use:

```bash
docker-compose down
```

## Getting Started

### 1. Airflow Setup:

Navigate to the deploy_airflow directory.

Use the provided docker-compose.yaml file to deploy Airflow locally:

```bash
docker-compose up -d
```
### 2. Run Scripts:

Execute the scripts in the scripts directory:
```bash
python scripts/create_tables.py
python scripts/initial_load.py
```
### 3. Explore Notebooks:

Refer to the notebooks in the notebooks directory for additional insights:
- `01_data_extraction.ipynb`
- `02_test_db_connection.ipynb`


## Additional Notes
- Ensure all dependencies are installed by referencing the requirements.txt file. Create a Virtual Environment and run the following command:

```bash
pip install -r requirements.txt
```

Feel free to reach out for any questions or assistance!