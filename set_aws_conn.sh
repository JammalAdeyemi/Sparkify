#!/bin/bash

source ~/airflow_venv/bin/activate
export AIRFLOW_HOME="/workspaces/Sparkify/airflow_home"

S3_BUCKET="${S3_BUCKET:-jammal-s3}"
S3_SONG_DATA="${S3_SONG_DATA:-song-data}"
S3_LOG_DATA="${S3_LOG_DATA:-log-data}"
S3_LOG_JSON="${S3_LOG_JSON:-log_json_path.json}"

echo "Setting up AWS + Redshift connection..."

# Delete existing connection
airflow connections delete aws_credentials 2>/dev/null || true
airflow connections delete redshift 2>/dev/null || true

# Create AWS connection using URI
airflow connections add aws_credentials \
    --conn-uri "aws://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@"

# Create Redshift connection using URI
airflow connections add redshift \
    --conn-uri "redshift://${REDSHIFT_USER}:${REDSHIFT_PASSWORD}@${REDSHIFT_HOST}:${REDSHIFT_PORT}/${REDSHIFT_DB}"

airflow variables set s3_bucket "$S3_BUCKET"
airflow variables set s3_song_data "$S3_SONG_DATA"
airflow variables set s3_log_data "$S3_LOG_DATA"
airflow variables set s3_log_json "$S3_LOG_JSON"

echo "✓ aws_credentials connection created & variables set"