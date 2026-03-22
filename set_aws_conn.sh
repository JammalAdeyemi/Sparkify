#!/bin/bash

source ~/airflow_venv/bin/activate
export AIRFLOW_HOME="/workspaces/Sparkify/airflow_home"

echo "Setting up AWS connection..."

# Delete existing connection
airflow connections delete aws_credentials 2>/dev/null || true
airflow connections delete redshift 1>/dev/null || true
airflow variables delete s3_bucket 1>/dev/null || true
airflow variables delete s3_prefix 1>/dev/null || true

# Create connection using URI
airflow connections add aws_credentials \
    --conn-uri "aws://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@"

# Create Redshift connection using URI
# Create Redshift connection using URI
airflow connections add redshift \
    --conn-uri "redshift://${REDSHIFT_USER}:${REDSHIFT_PASSWORD}@${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DB}"

airflow variables set s3_bucket "jammals3"
airflow variables set s3_prefix "data-pipelines"

echo "✓ aws_credentials connection created & variables set"