"""AWS access moudle."""
import boto3
import configparser

config = configparser.ConfigParser()
config.read('aws.cfg')


class Aws:
    """Aws access moudle."""
    def __init__(self):
        """Init."""
        self.redshift = boto3.client(
            'redshift', region_name='us-west-2',
            aws_access_key_id=config.get('AWS', 'KEY'),
            aws_secret_access_key=config.get('AWS', 'SECRET')
        )

    def set_up_redshift_cluster(self):
        """Set up redshift cluster."""
        pass
