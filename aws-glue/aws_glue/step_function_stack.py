from aws_cdk import (
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_iam as iam,
    Stack,
)
from constructs import Construct


class StepFunctionStack(Stack):
  
    def __init__(self, scope: Construct, construct_id: str, glue_crawler_name: str, step_function_role_arn: str, glue_etl_job_name: str, notification_email: str, step_function_name:str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Use the existing IAM Role for the Step Function
        step_function_role = iam.Role.from_role_arn(
            self,
            "ExistingStepFunctionRole",
            role_arn=step_function_role_arn,
            mutable=False 
        )

        # Create SNS topics for notifications
        etl_job_topic = sns.Topic(self, "ETLJobTopic",
            topic_name="etljobtopic"
        )
        crawler_topic = sns.Topic(self, "CrawlerTopic",
            topic_name="crawlertopic"
        )

        # Add email subscriptions to SNS topics
        etl_job_topic.add_subscription(subscriptions.EmailSubscription(notification_email))
        crawler_topic.add_subscription(subscriptions.EmailSubscription(notification_email))

        # Define a Step Function task to run the Glue Crawler with sync behavior
        run_crawler_task = tasks.CallAwsService(
            self,
            "StartCrawler",
            service="glue",
            action="startCrawler",
            parameters={
                "Name": glue_crawler_name
            },
            iam_resources=["*"],  # Provide necessary IAM resources
            result_path="$.crawlerResult",
        )

        # Define a Step Function task to run the Glue ETL Job (no integration pattern required)
        run_etl_task = tasks.GlueStartJobRun(
            self,
            "RunETLJob1",
            glue_job_name=glue_etl_job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB, 
        )

        # Define a Step Function task to send SNS notification after Glue ETL Job completion
        notify_etl_completion = tasks.SnsPublish(
            self,
            "SendSNSNotificationForETL",
            topic=etl_job_topic,
            message=sfn.TaskInput.from_text("ETL Job completed successfully."),
            result_path="$.snsResult",
        )

        # Define a Step Function task to send SNS notification after Crawler completion
        notify_crawler_completion = tasks.SnsPublish(
            self,
            "SendSNSNotificationForCrawler",
            topic=crawler_topic,
            message=sfn.TaskInput.from_text("Crawler started successfully."),
            result_path="$.snsResult",
        )

        # Parallel state to run both tasks (ETL Job and Crawler) at the same time
        parallel_tasks = sfn.Parallel(self, "RunETLAndCrawlerInParallel")
        parallel_tasks.branch(
            run_etl_task.next(notify_etl_completion)
        )
        parallel_tasks.branch(
            run_crawler_task.next(notify_crawler_completion)
        )

        # Create the State Machine to orchestrate the tasks
        state_machine = sfn.StateMachine(
            self,
            "GlueStepFunction",
            definition=parallel_tasks,
            role=step_function_role,  # Use the existing role
            state_machine_name=step_function_name 
        )
