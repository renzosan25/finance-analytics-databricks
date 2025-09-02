CREATE EXTERNAL LOCATION gold_transactions
URL 's3://finance-dataset-bucket/gold_dataset/'
WITH (STORAGE CREDENTIAL s3_use_case);

CREATE SCHEMA IF NOT EXISTS finance_datasets
MANAGED LOCATION 's3://finance-dataset-bucket/gold_dataset/';