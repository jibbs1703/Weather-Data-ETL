import os
from io import StringIO
from dotenv import load_dotenv
import logging
import boto3
from botocore.exceptions import ClientError
import tempfile


class S3Buckets:
    @classmethod
    def credentials(cls, region=None):
        """
        The credentials @classmethod runs when the S3Buckets class is initialized. This method accesses
        the secret and access keys for the user. These keys are specified in a hidden env file in the
        directory. When a region is specified, the S3 Bucket methods called are executed in the specified
        region and if no region is specified, AWS assigns a region while using the services.
        the region argument

        :param region:
        :return: secret key, access key, region specified by user
        """
        # Load the User's Access and Secret Key from .env file
        load_dotenv()
        secret = os.getenv("ACCESS_SECRET")
        access = os.getenv("ACCESS_KEY")
        return cls(secret, access, region)

    def __init__(self, secret, access, region):
        """
        The __init__ method for the S3Buckets class creates the client for accessing the user's AWS account.
        The client is created using the boto3 module and is made globally available in the S3Buckets class
        for subsequent methods in the class.

        :param secret: user secret key (loaded from .env file)
        :param access: ser access key (loaded from .env file)
        :param region: specified region during instantiation of class
        """
        if region is None:
            self.client = boto3.client('s3', aws_access_key_id=access, aws_secret_access_key=secret)
            print(secret, access, region)
        else:
            self.location = {'LocationConstraint': region}
            self.client = boto3.client('s3', aws_access_key_id=access, aws_secret_access_key=secret, region_name=region)

    def list_buckets(self):
        """
        This method returns a list of bucket names available in the user's AWS account.

        :return: a list of the s3 bucket instances present in the accessed account
        """
        response = self.client.list_buckets()
        buckets = response["Buckets"]
        all_buckets = [bucket["Name"] for bucket in buckets]
        return all_buckets

    def create_bucket(self, bucket_name):
        """
        This method creates an S3 bucket in the user's AWS account in the region specified while
        instantiating the class. A new bucket is created if the bucket name passed into the
        method has not been previously created. If a region is not specified, the bucket is
        created in the S3 default region (us-east-1).

        :param bucket_name:
        """
        if bucket_name in self.list_buckets():
            print(f"The bucket {bucket_name} already exists")
            pass
        else:
            print("A new bucket will be created in your AWS account")
            self.client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=self.location)
            print(f"The bucket {bucket_name} has been successfully created")

    def upload_file(self, file_name, bucket_name, object_name=None):
        """
        This method uploads a file to an S3 bucket in the user's AWS account

        :param file_name: File to upload to S3 Bucket
        :param bucket_name: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """
        # If S3 object_name is not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)
        try:
            self.client.upload_file(file_name, bucket_name, object_name)

        except ClientError as e:
            logging.error(e)
            return False
        return True

    def download_file(self, bucket_name, object_name, file_name):
        """
        This method downloads a file from an S3 bucket in the user's AWS account.

        :param bucket_name: Bucket to download file from
        :param object_name: File to download from S3 Bucket
        :param file_name: Filename to save the file to.
        :return: Downloaded file
        """
        file = self.client.download_file(bucket_name, object_name, file_name)
        return file

    def read_file(self, bucket_name, object_name):
        """
        This method reads a file from an S3 bucket in the user's AWS account, returns an object
        containing the file read.

        :param bucket_name: Bucket to read file from
        :param object_name: File to read from S3 Bucket
        :return: an object containing the file read from the S3 Bucket.
        """
        response = self.client.get_object(Bucket=bucket_name, Key=object_name)
        file = StringIO(response['Body'].read().decode('utf-8'))
        return file

    def upload_dataframe_to_s3(self, df, bucket_name, object_name):
        """
        This method uploads a pandas dataframe to an S3 bucket in the user's AWS account as a.csv file

        :param df: dataframe to upload to S3 Bucket
        :param bucket_name: Bucket to upload dataframe to
        :param object_name: name dataframe takes in the is user's s3 bucket.
        :return: Success message if file was uploaded.
        """
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, header=True, index=False)
        self.client.put_object(Bucket=bucket_name, Body=csv_buffer.getvalue(), Key = object_name)
        print("Dataframe is saved as CSV in S3 bucket.")