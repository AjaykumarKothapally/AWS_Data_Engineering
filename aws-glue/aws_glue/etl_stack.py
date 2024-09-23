from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_glue as glue,
    aws_iam as iam,
    CfnOutput
)
from constructs import Construct


class EtlStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, glue_role_arn: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

       
        source_bucket = s3.Bucket.from_bucket_name(self, "SourceBucket", "demo-source-ap-south-1-1809")
        transformed_bucket = s3.Bucket.from_bucket_name(self, "TransformedBucket", "demo-transform-ap-south-1-1809")

        
        glue_role = iam.Role.from_role_arn(self, "GlueRole", role_arn=glue_role_arn)

        
        etl_script_location = "s3://etl-scripts-bucket-1909/etl_script.py"

       
        glue_job = glue.CfnJob(
            self,
            "GlueEtlJob",
            role=glue_role.role_arn,
            command={
                "name": "glueetl",  
                "scriptLocation": etl_script_location,  
                "pythonVersion": "3"  
            },
            default_arguments={
                "--job-bookmark-option": "job-bookmark-enable",  
                "--enable-metrics": "",  
                "--source_bucket": source_bucket.bucket_name,  
                "--destination_bucket": transformed_bucket.bucket_name 
            },
            max_retries=1,
            timeout=10,
            glue_version="3.0",
            number_of_workers=2,
            worker_type="Standard",
            name="etl-job"
        )

        # Outputs
        CfnOutput(self, "GlueJobName", value=glue_job.ref)
