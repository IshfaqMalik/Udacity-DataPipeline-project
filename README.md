
# Data Pipeline with Apache Airflow

This repository contains a data pipeline project that demonstrates how to automate data workflows using **Apache Airflow**. The pipeline extracts JSON-formatted log and song data stored in Amazon S3, loads it into staging tables in Amazon Redshift, and transforms the data to populate analytics tables for querying and analysis.

---

## Project Overview

This project is designed as part of a data engineering workflow. The pipeline:

1. **Extracts**:
   - Loads raw data from an S3 bucket (e.g., `log-data/` and `song-data/`) into Redshift staging tables.
2. **Transforms**:
   - Applies SQL transformations to populate fact and dimension tables in Redshift.
3. **Validates**:
   - Ensures the quality of the data loaded into the Redshift tables.

The result is a robust and scalable data pipeline that enables analysts to query data efficiently for reporting and analytics.

---

## Architecture

The pipeline uses the following components:

- **Apache Airflow**: Orchestrates the tasks within the pipeline.
- **Amazon S3**: Stores raw log and song data in JSON format.
- **Amazon Redshift**: Serves as the data warehouse.
- **Custom Operators**: Written in Python to handle staging, transformation, and data validation.

---

## Data Pipeline Workflow

1. **Staging**:
   - The `StageToRedshiftOperator` loads raw JSON data from S3 into Redshift staging tables (`staging_events` and `staging_songs`).
2. **Fact Table**:
   - The `LoadFactOperator` populates the `songplays` fact table by joining the staging tables.
3. **Dimension Tables**:
   - The `LoadDimensionOperator` populates the `users`, `songs`, `artists`, and `time` dimension tables.
4. **Data Validation**:
   - The `DataQualityOperator` ensures that critical tables are not empty and validates specific conditions.

---

## File Structure

```
.
├── dags/
│   ├── final_project.py                # Main DAG file
├── plugins/
│   ├── operators/
│   │   ├── stage_redshift.py           # StageToRedshiftOperator
│   │   ├── load_fact.py                # LoadFactOperator
│   │   ├── load_dimension.py           # LoadDimensionOperator
│   │   ├── data_quality.py             # DataQualityOperator
│   ├── helpers/
│   │   ├── final_project_sql_statements.py # SQL queries for transformations
├── README.md                           # Project documentation
```

---

## Operators Overview

### 1. `StageToRedshiftOperator`
- **Purpose**: Loads raw JSON data from S3 into Redshift staging tables.
- **Parameters**:
  - `redshift_conn_id`: Airflow connection ID for Redshift.
  - `aws_credentials_id`: Airflow connection ID for AWS credentials.
  - `table`: Target Redshift table.
  - `s3_bucket`: S3 bucket name.
  - `s3_key`: S3 key prefix.
  - `json_path`: JSONPaths file or `auto`.

### 2. `LoadFactOperator`
- **Purpose**: Populates the fact table (`songplays`) by transforming staging data.
- **Parameters**:
  - `redshift_conn_id`: Airflow connection ID for Redshift.
  - `table`: Target fact table name.
  - `sql`: SQL query for the transformation.

### 3. `LoadDimensionOperator`
- **Purpose**: Populates dimension tables (`users`, `songs`, `artists`, `time`).
- **Parameters**:
  - `redshift_conn_id`: Airflow connection ID for Redshift.
  - `table`: Target dimension table name.
  - `sql`: SQL query for the transformation.
  - `append_only`: Boolean flag to control whether the table is truncated before insertion.

### 4. `DataQualityOperator`
- **Purpose**: Runs data quality checks to ensure data integrity.
- **Parameters**:
  - `redshift_conn_id`: Airflow connection ID for Redshift.
  - `test_cases`: List of SQL queries and expected results for validation.

---

## How to Run

### Prerequisites
- Python 3.7 or above
- Apache Airflow
- AWS Account with:
  - S3 bucket access
  - Redshift cluster

### Steps
1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```

2. Set up Airflow:
   ```bash
   export AIRFLOW_HOME=~/airflow
   airflow db init
   airflow webserver
   airflow scheduler
   ```

3. Create Airflow Connections:
   - **Redshift**: Add a connection in Airflow with the following details:
     - Conn ID: `redshift`
     - Host, Schema, Login, Password: Your Redshift details.
   - **AWS Credentials**: Add a connection with:
     - Conn ID: `aws_credentials`
     - Login: AWS Access Key
     - Password: AWS Secret Key

4. Place the DAG:
   - Copy the `final_project.py` file to the `dags/` folder.

5. Run the DAG:
   - Trigger the DAG in the Airflow UI or CLI:
     ```bash
     airflow dags trigger final_project
     ```

---

## Example Queries

### Songplays Fact Table
Query to get the total number of songplays:
```sql
SELECT COUNT(*) FROM songplays;
```

### User Table
Query to get the number of distinct users:
```sql
SELECT COUNT(DISTINCT userid) FROM users;
```

---

## Future Improvements

1. Automate S3 path generation using execution date.
2. Add more advanced data validation steps.
3. Monitor pipeline performance using Airflow's task duration metrics.

---

## License

This project is licensed under the [MIT License](LICENSE).

---
