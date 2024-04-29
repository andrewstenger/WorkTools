#!/usr/bin/env bash

REGION="us-east-1"
QUEUE="job-queue"
MAX_ITEMS=1000000

for i in $(aws --region ${REGION} batch list-jobs --job-queue ${QUEUE} --job-status runnable --output text --max-items ${MAX_ITEMS} --query jobSummaryList[*].[jobId])
do
  echo "Cancel Job: $i"
  aws --region ${REGION} batch cancel-job --job-id $i --reason "Cancelling job."
done
