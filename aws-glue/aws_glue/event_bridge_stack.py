from aws_cdk import (
    CfnOutput,
    aws_s3 as s3,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_stepfunctions as sfn,
    Stack,
)
from constructs import Construct


class EventBridgeStack(Stack):

    def __init__(
        self,
        scope: Construct,
        id: str,
        source_bucket_name: str,
        step_function_arn: str,
        **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # Reference to the existing S3 bucket
        source_bucket = s3.Bucket.from_bucket_name(
            self, "SourceBucket", bucket_name=source_bucket_name
        )

        # Reference to the existing Step Function
        step_function = sfn.StateMachine.from_state_machine_arn(
            self, "StepFunction", state_machine_arn=step_function_arn
        )

        # Create the EventBridge rule for S3 Object Created event
        rule = events.Rule(
            self,
            "S3ObjectCreatedRule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={"bucket": {"name": [source_bucket.bucket_name]}},
            ),
        )
        
         # Set Step Function as the target of the EventBridge rule
        rule.add_target(targets.SfnStateMachine(step_function))
        
        
        # Grant EventBridge permission to invoke the Step Function
        step_function.grant_start_execution(
            iam.ServicePrincipal("events.amazonaws.com")
        )

        # Output the EventBridge rule ARN
        CfnOutput(self, "EventBridgeRuleArn", value=rule.rule_arn)
