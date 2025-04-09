import boto3
from botocore.exceptions import ClientError

class S3Client:
    def __init__(self):
        self.client = None
        
    def init_app(self, app):
        self.client = boto3.client(
            's3',
            aws_access_key_id=app.config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=app.config['AWS_SECRET_ACCESS_KEY'],
            region_name=app.config['AWS_REGION']
        )
        self.bucket = app.config['S3_BUCKET']

s3 = S3Client()