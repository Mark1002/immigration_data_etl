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
        self.s3_bucket = config.get('S3', 'BUCKET_NAME')

    def upload_immigration_data(self):
        """Upload immigration sas file."""
        dir_path = '../../data/18-83510-I94-Data-2016/'
        for file in os.listdir(dir_path):
            self.s3_client.upload_file(
                f'{dir_path}{file}', self.s3_bucket,
                f'data/18-83510-I94-Data-2016/{file}'
            )

    def upload_cities_demographics_data(self):
        """Upload us-cities-demographics.csv."""
        self.s3_client.upload_file(
            'us-cities-demographics.csv', self.s3_bucket,
            'data/us-cities-demographics.csv'
        )

    def upload_mapping_data(self):
        """Upload mapping data."""
        for file in os.listdir('mapping'):
            self.s3_client.upload_file(
                file, self.s3_bucket, f'data/mapping/{file}'
            )

    def upload_airport_data(self):
        """Upload airport-codes_csv.csv."""
        self.s3_client.upload_file(
            'airport-codes_csv.csv', self.s3_bucket,
            'data/airport-codes_csv.csv'
        )


def main():
    """Main execute."""
    s3 = S3UploadModule()
    s3.upload_airport_data()
    s3.upload_cities_demographics_data()
    s3.upload_immigration_data()
    s3.upload_mapping_data()


if __name__ == '__main__':
    main()
