#!/bin/sh
# Create the BigQuery dataset and tables if they do not exist (the analytics
# analogue of running migrations on start), then serve. With the in-memory
# backend this is a no-op.
python -m app.admin ensure
uvicorn app.main:app --host 0.0.0.0 --port 8080
