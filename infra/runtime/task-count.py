import os

import boto3

ecs = boto3.client("ecs")
cloudwatch = boto3.client("cloudwatch")


def handler(event, context):
    cluster = os.environ["CLUSTER"]
    service = os.environ["SERVICE"]
    response = ecs.describe_services(cluster=cluster, services=[service])
    services = response.get("services", [])
    count = services[0]["runningCount"] if services else 0
    cloudwatch.put_metric_data(
        Namespace="Capstan/Service",
        MetricData=[{
            "MetricName": "RunningTaskCount",
            "Dimensions": [
                {"Name": "ClusterName", "Value": cluster},
                {"Name": "ServiceName", "Value": service},
            ],
            "Unit": "Count",
            "Value": count,
        }],
    )
