import aws_cdk as core
import aws_cdk.assertions as assertions

from aws_glue.aws_glue_stack import AwsGlueStack

# example tests. To run these tests, uncomment this file along with the example
# resource in aws_glue/aws_glue_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = AwsGlueStack(app, "aws-glue")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
