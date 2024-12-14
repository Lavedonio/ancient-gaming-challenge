# Ancient Gaming Data Engineering challenge
Data Engineering Technical Challenge proposed by Ancient Gaming.

### Notes
- The proposed challenge was sent via email.
- Instructions on how to log in to the Google Cloud Provider (GCP) account was sent back via email.
- The queries required by the challenge are available in the final processing stage (level 5 - comsumption).

## Architecture
A brief overview of the project architecture.

### Services used
#### Google Cloud Composer
Google Cloud Composer is used to automate and fully manage the Apache Airflow instance for this project. This is a key part of the pipeline, since it orchestrates the entire process and enables pre-processing tasks to be executed prior to the DBT ones. Apache Airflow is also an easy and robust way to orchestrate data pipelines, so that's why it's used.

#### Google Cloud Storage
Google Cloud Storage is used to store both the relevant files for Google Cloud Composer, as well as a storage solution for the raw data before being processed. This is a easy, secure, safe and robust was to store raw data to be inserted later in BigQuery.

#### BigQuery
BigQuery is used as the backend of the DBT pipeline, ensuring fast and reliable data transformation, being the ideal place to have the Data Warehouse.

#### IAM
Identity and Access Management (IAM) was used to give the right permissions to all components and users to access the project.

### Pipeline structure
The pipeline contains 6 stages, each separated in the Aiflow DAG by their own Task Group. They are:

1. Pre loading
2. Level 1 - landing
3. Level 2 - source
4. Level 3 - intermediate
5. Level 4 - final
6. Level 5 - consumption

#### Pre loading
This part takes care of creating the data in the raw files and uploading them to Google Cloud Composer, where they will be loaded into BigQuery in the next step.

This is the part that would be the most diferent in a real production environment. In that case, the files wouldn't be generated on the spot, but rather be added through a different service, or even added to BigQuery directly via Fivetran or Stitch, for instance, skipping this process altogether.

#### Level 1 - landing
This is the first stage of the data in BigQuery. Here, the raw data is loaded as is and partitioned by the execution date that comes from Airflow. This part is crucial to make sure that the pipeline is idempotent and can be backfilled in the future if needed.

#### Level 2 - source
This is the second stage of the data in BigQuery. Here, the raw data from the previous stage is cleaned so that this will be the first clean slate for the following processing steps.

#### Level 3 - intermediate
This is the third stage of the data in BigQuery. Here, the source data is processed and intermediate tables are created to be used in multiple final tables or to make it easier to maintain a more complex logic by dividing it in multiple stages. This is not a necessary step and some data flows go directly from source to final.

#### Level 4 - final
This is the forth stage of the data in BigQuery. Here, the final queries are created to then feed both reports and dashboards that will be used by the Data Analysts or be consumed by other services.

#### Level 5 - consumption
This is the fifth and final stage of the data in BigQuery. Here, the there's no extra processing and all the final tables are presented here by simply selecting all fields. The reason for this stage to be present is to be the target for all further processing and being the connection point for data visualization tools. That way, if there's a data source migration in the future, this stage can behave as a valve to which data stream goes to the reports, making sure the Data Engineers can control the flow and make adjustments if necessary without any extra work from our stakeholders downstream.

## Final notes
For a challenge which the deadline was just a few days, the solution proposed is robust and could be a POC for a production-level implementation. Given that, there's room for improvement in this project.

1. There's a bug in the implementation where the column where the landing tables are partitioned by are not being populated, meaning they are all null.
2. A CI/CD implementation would be the obvious next step when it comes to a project like this. Right now the files need to be manually copied to the composed bucket, but a CI/CD approach could do that automatically.
3. The DBT project does have schema tests, but they are not active for now. Since I've used the cheapest machine for this project, the total time the pipeline took to complete a run was far too long, so they were removed for now. By increasing the size of the Composer instance, there would be more room to alocate more resources to that, making it posible to re-enable the tests.
