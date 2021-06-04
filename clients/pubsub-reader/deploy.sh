#!/usr/bin/env bash

gcloud functions deploy raspberry-events \
    --project danarchy-io \
    --runtime python37 \
    --memory 128MB \
    --entry-point main \
    --max-instances 1 \
    --trigger-topic raspberry-events