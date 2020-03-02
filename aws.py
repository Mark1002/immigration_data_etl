"""AWS access module."""
import boto3
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read('aws.cfg')


class RedshiftModule:
    """redshift access module."""

    def __init__(self):
        """Init."""
        self.redshift = boto3.client(
            'redshift', region_name='us-west-2',
            aws_access_key_id=config.get('AWS', 'KEY'),
            aws_secret_access_key=config.get('AWS', 'SECRET')
        )
        self.iam = boto3.client(
            'iam', region_name='us-west-2',
            aws_access_key_id=config.get('AWS', 'KEY'),
            aws_secret_access_key=config.get('AWS', 'SECRET')
        )

    def prettyRedshiftProps(self, props):
        pd.set_option('display.max_colwidth', -1)
        keysToShow = [
            "ClusterIdentifier", "NodeType", "ClusterStatus",
            "MasterUsername", "DBName", "Endpoint",
            "NumberOfNodes", 'VpcId'
        ]
        x = [(k, v) for k, v in props.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])

    def set_up_redshift_cluster(self):
        """Set up redshift cluster."""
        try:
            # get i am role
            roleArn = self.iam.get_role(
                RoleName='myRedshiftRole'
            )['Role']['Arn']
            self.redshift.create_cluster(
                ClusterType='multi-node',
                NodeType='dc2.large',
                NumberOfNodes=4,
                DBName=config.get('CLUSTER', 'DB_NAME'),
                ClusterIdentifier=config.get('CLUSTER', 'DB_USER'),
                MasterUsername=config.get('CLUSTER', 'DB_USER'),
                MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD'),
                IamRoles=[roleArn]
            )
        except Exception as e:
            print(e)
