
import uuid
import os
import boto3
import json
import datetime as dt
import botocore
import multiprocessing as mp


def make_job(job_dict):
    batch = boto3.client('batch', region_name='us-east-1')
    print(json.dumps(job_dict, indent=4))
    batch.submit_job(**job_dict)


def job_parameters(retries, cpu, memory, latest_job_def, job_name, queue):
    hex_hash = uuid.uuid4().hex[:8]
    job_dict = {
        "jobName" : f"{job_name}-{hex_hash}",
        "jobDefinition" : latest_job_def,
        "jobQueue" : queue,
        "retryStrategy" : {
            "attempts": retries
        },
        "containerOverrides": {
            "command" : ["python", "-u", "process_script.py"],
            "resourceRequirements": [
                {
                    "type": "VCPU",
                    "value": str(cpu)
                },
                {
                    "type": "MEMORY",
                    "value": str(memory*1000)
                },
            ],
            "environment": [
                {
                    "name": "THREADS",
                    "value": str(cpu),
                },
            ],
        }
    }
    return job_dict


def latest_job_definition(jobDefinitionName):
    # Get latest job definition from Batch
    batch = boto3.client('batch', region_name='us-east-1')
    paginator = batch.get_paginator("describe_job_definitions")
    pages = paginator.paginate(jobDefinitionName=jobDefinitionName)
    arns = {} 
    for page in pages: 
        for definition in page['jobDefinitions']:
            arns[definition['jobDefinitionArn']] = definition['revision']
    latest_arn = max(arns, key=arns.get) 
    return latest_arn


def make_queue(execution_id, compute_environment):
    batch = boto3.client('batch', region_name='us-east-1')
    queue_params = {
        "jobQueueName": execution_id,
        "priority": 1,
        "computeEnvironmentOrder": [
            {
                "order": 1,
                "computeEnvironment": compute_environment
            }
        ]
    }
    try:
        batch.create_job_queue(**queue_params)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'ClientException':
            pass
        else:
            raise e

    # Wait for queue to be created, other wise error is raised when a job is submitted to a non-existent queue
    while True:
        response = batch.describe_job_queues(jobQueues=[execution_id])
        queue_status = response['jobQueues'][0]['status']
        if queue_status == "VALID":
            break



if __name__=="__main__":

    testing = True
    test_size = 3

    queue = 'queue'
    compute_env = 'compute-env'
    make_queue(queue, compute_env)

    cpu = 4
    memory = 16
    data = []

    latest_job_def = latest_job_definition('job-def')
    execution_id   = dt.datetime.now().strftime("%Y%m%d%H%M%S")

    for i, d in enumerate(data):
        param = {
            'retries'           : 1,
            'cpu'               : cpu,
            'memory'            : memory,
            # ...
        }
        make_job(
            job_parameters(**param)
        )

        if testing and i >= test_size - 1:
            break
