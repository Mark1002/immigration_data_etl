"""AWS s3 upload module."""
import os
import boto3
import configparser

config = configparser.ConfigParser()
config.read('aws.cfg')
os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS', 'KEY')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS', 'SECRET')


class S3UploadModule:
    """S3 upload module."""

    def __init__(self):
        """Init."""
        self.s3_client = boto3.client('s3')

    def upload_immigration_data(self):
        """Upload immigration sas file."""
        pass

    def upload_cities_demographics_data(self):
        """Upload us-cities-demographics.csv."""
        pass

    def upload_mapping_data(self):
        """Upload mapping data."""
        pass

    def upload_airport_data(self):
        """Upload airport-codes_csv.csv."""
        pass
