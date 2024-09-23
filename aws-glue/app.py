#!/usr/bin/env python3
import os
import aws_cdk as cdk
from aws_glue.aws_glue_stack import AwsGlueStack
from aws_glue.etl_stack import EtlStack
from aws_glue.step_function_stack import StepFunctionStack
from aws_glue.event_bridge_stack import EventBridgeStack

app = cdk.App()

glue_stack = AwsGlueStack(
    app,
    "AwsGlueStack",
)

EtlStack(app, "EtlStack", glue_role_arn=glue_stack.glue_role.role_arn)

StepFunctionStack(
    app,
    "StepFunctionStack",
    glue_crawler_name="demo-crawler-name",
    glue_etl_job_name="etl-job",
    notification_email="a.kothapally@nitcoinc.in",
    step_function_role_arn="arn:aws:iam::905418476815:role/stepfunctionexecutionrole",
    step_function_name="my-stepfunction-machine",
)

EventBridgeStack(
    app,
    "EventBridgeStack",
    source_bucket_name="demo-source-ap-south-1-1809",
    step_function_arn="arn:aws:states:ap-south-1:905418476815:stateMachine:my-stepfunction-machine",
)

app.synth()
