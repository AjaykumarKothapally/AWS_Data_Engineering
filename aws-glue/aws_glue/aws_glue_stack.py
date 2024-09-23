from aws_cdk import (
    aws_s3 as s3,
    aws_glue as glue,
    aws_iam as iam,
    Stack,
    App,
    CfnOutput,
)


class AwsGlueStack(Stack):
    def __init__(self, scope: App, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        # Create S3 buckets
        s3_source_bucket = s3.Bucket(
            self, "SourceBucket", bucket_name="demo-source-ap-south-1-1809"
        )
        s3_transformed_bucket = s3.Bucket(
            self, "TransformedBucket", bucket_name="demo-transform-ap-south-1-1809"
        )

        # Create IAM role for Glue
        self.glue_role = iam.Role(
            self,
            "GlueRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
            ],
            role_name="CustomGlueRoleName",
        )

    
        # Create Glue Database
        glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input={
                "name": "my_database",
                "description": "A Glue database for storing metadata",
            },
        )

        # Create Glue Crawler
        glue_crawler = glue.CfnCrawler(
            self,
            "GlueCrawler",
            role=self.glue_role.role_arn,
            database_name=glue_database.ref,
            name="demo-crawler-name",
            targets={
                "s3Targets": [
                    {
                        "path": s3_source_bucket.bucket_name  
                    }
                ]
            },
            description="Crawler to crawl data from the source bucket",
        )

        # Outputs
        CfnOutput(self, "SourceBucketArn", value=s3_source_bucket.bucket_arn)
        CfnOutput(self, "TransformedBucketArn", value=s3_transformed_bucket.bucket_arn)
        CfnOutput(self, "GlueRoleArn", value=self.glue_role.role_arn)
        CfnOutput(self, "GlueDatabaseName", value=glue_database.ref)
