# Data Pipelines with Airflow

---

## Overview
This project consists in ELT data pipelines using Apache Airflow for a music streaming company, Sparkify,
to automate and monitor their data warehouse. The data pipelines are dynamic and built from reusable tasks,
which can be monitored, and allow easy backfills. 

Include data quality checks executed on top the data warehouse to run tests against their datasets after the ETL
steps have been executed.

The source data resides in S3 is processed in Sparkify's data warehouse in Amazon Redshift. The source datasets
consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

---

## How to run:

### Prerequisites:
-Create an IAM User in AWS with appropriate permissions.
-Create a redshift cluster in AWS. Ensure that you are creating this cluster in the us-west-2 region since the S3
bucket used is located in us-west-2.

[Start](https://airflow.apache.org/docs/apache-airflow/1.10.12/start.html) an Airflow web server with the compatible version.

Once in the airflow web UI:
- add Redshift and AWS credentials to the connections by going to Admin -> connections -> Create
- run create_tables dag and wait for the dag to finish
- run load_data dag (this might take a long time to complete all the tasks)
