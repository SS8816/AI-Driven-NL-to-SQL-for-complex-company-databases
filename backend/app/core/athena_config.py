import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from pydantic import BaseModel, Field, validator

class Config(BaseModel):
    """
    Holds configuration for the Athena client, loaded from environment variables.
    Includes validation for S3 paths and AWS credentials.
    """
    aws_region: str = Field(default=os.getenv("AWS_REGION", "us-east-1"))
    s3_output_location: str = Field(default=os.getenv("ATHENA_S3_OUTPUT_LOCATION", "s3://fastmap-athena-workgroup/"))
    athena_workgroup: str = Field(default=os.getenv("ATHENA_WORKGROUP", "primary"))
    timeout_seconds: int = Field(default=int(os.getenv("ATHENA_TIMEOUT_SECONDS", "1800")))

    @validator('s3_output_location')
    def validate_s3_path(cls, v):
        if not v:
            raise ValueError("ATHENA_S3_OUTPUT_LOCATION environment variable is required.")
        if not v.startswith("s3://"):
            raise ValueError(f"ATHENA_S3_OUTPUT_LOCATION must be a valid S3 path. Got: {v}")
        return v

    def validate_credentials(self):
        """Checks if AWS credentials are configured and valid."""
        try:
            session = boto3.Session(region_name=self.aws_region)
            client = session.client("sts")
            client.get_caller_identity()
            print("AWS credentials validated successfully.")
        except NoCredentialsError:
            raise ConnectionError("AWS credentials not found. Please configure them.")
        except ClientError as e:
            raise ConnectionError(f"AWS credentials invalid or permissions insufficient: {e}")