from economic_data import config
import os

config.initialize(s3_bucket_name=os.getenv("S3_BUCKET_NAME"))
