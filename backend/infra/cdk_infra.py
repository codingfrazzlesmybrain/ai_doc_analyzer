from aws_cdk import aws_iam as iam
from aws_cdk import (
    Stack,
    App,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    Duration,
)

class AiDocAnalyzerStack(Stack):
    def __init__(self, scope: App, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Reference the existing S3 Bucket
        bucket = s3.Bucket.from_bucket_name(self, "AiDocAnalyzerBucket",
                                           bucket_name="ai-doc-records-lee-b")

        # Define Lambda Function
        lambda_function = _lambda.Function(self, "AiDocAnalyzerLambda",
                                          runtime=_lambda.Runtime.PYTHON_3_9,
                                          handler="lambda_handler.lambda_handler",
                                          code=_lambda.Code.from_asset("../lambda"),
                                          environment={"BUCKET_NAME": bucket.bucket_name},
                                          timeout=Duration.seconds(30))

        lambda_function.role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "comprehend:DetectSentiment",
                    "comprehend:DetectEntities",
                    "comprehend:DetectKeyPhrases",
                    "comprehend:DetectDominantLanguage",
                    "textract:DetectDocumentText",
                    "textract:StartDocumentTextDetection",
                    "textract:GetDocumentTextDetection"
                ],
                resources=["*"]
            )
        )

        # Grant Lambda permission to read/write to the existing S3 bucket
        bucket.grant_read_write(lambda_function)

        # Add a single S3 trigger for uploads with no specific suffix
        bucket.add_event_notification(s3.EventType.OBJECT_CREATED,
                                      s3n.LambdaDestination(lambda_function),
                                      s3.NotificationKeyFilter(prefix="uploads/"))

        # Add Textract permission to the Lambda role
        lambda_function.role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=["textract:DetectDocumentText"],
                resources=[f"arn:aws:s3:::{bucket.bucket_name}/*"],
                effect=iam.Effect.ALLOW
            )
        )

app = App()
AiDocAnalyzerStack(app, "AiDocAnalyzerStack")
app.synth()