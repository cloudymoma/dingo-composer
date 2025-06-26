#!/bin/bash

export DAG_DIR=$(gcloud composer environments describe dingo-composer --location us-central1 --format="get(config.dagGcsPrefix)")

gcloud storage cp spark-batches.py $DAG_DIR
