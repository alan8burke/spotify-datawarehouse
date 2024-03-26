import configparser
import json
import time

import boto3
import fire
import requests
from botocore.exceptions import ClientError

CONFIG_FILENAME = "dwh.cfg"
config = configparser.ConfigParser()
config.read_file(open(CONFIG_FILENAME))
ACCESS_KEY_ID = config.get("AWS", "ACCESS_KEY_ID")
SECRET_ACCESS_KEY = config.get("AWS", "SECRET_ACCESS_KEY")
AWS_REGION = config.get("AWS", "AWS_REGION")
CLUSTER_ID = config.get("CLUSTER", "cluster_id")
DB_NAME = config.get("CLUSTER", "DB_NAME")
DB_USER = config.get("CLUSTER", "DB_USER")
DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DB_PORT = int(config.get("CLUSTER", "DB_PORT"))

iam = boto3.client(
    "iam",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

ec2_client = boto3.client(
    "ec2",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

ec2_resource = boto3.resource(
    "ec2",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

redshift = boto3.client(
    "redshift",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)


def create_iam_role(iam: boto3.client, iam_RoleName: str = "dwh_iam_role") -> str:
    """
    Creates an IAM role for AWS Redshift with the specified name.

    Args:
        iam (boto3.client): Boto3 IAM client object.
        iam_RoleName (str): Name of the IAM role to be created. Defaults to "dwh_iam_role".

    Returns:
        str: ARN (Amazon Resource Name) of the created IAM role.

    Raises:
        Exception: If there is an error creating the IAM role or attaching the policy.

    Note:
        This function assumes that appropriate permissions are already configured to create IAM roles.
    """
    try:
        print(f"Creating a new IAM Role: {iam_RoleName}")
        dwhRole = iam.create_role(
            Path="/",
            RoleName=iam_RoleName,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Statement": [
                        {
                            "Action": "sts:AssumeRole",
                            "Effect": "Allow",
                            "Principal": {"Service": "redshift.amazonaws.com"},
                        }
                    ],
                    "Version": "2012-10-17",
                }
            ),
        )
    except Exception as e:
        print(e)

    iam.attach_role_policy(
        RoleName=iam_RoleName,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )["ResponseMetadata"]["HTTPStatusCode"]

    roleArn = iam.get_role(RoleName=iam_RoleName)["Role"]["Arn"]
    print(f"The ARN of the IAM role was successfully created!\n{roleArn}")

    return roleArn


def delete_iam_role(iam: boto3.client, iam_RoleName: str = "dwh_iam_role"):
    """
    Deletes the specified IAM role.

    Args:
        iam (boto3.client): Boto3 IAM client object.
        iam_RoleName (str): Name of the IAM role to be deleted. Defaults to "dwh_iam_role".

    Returns:
        dict: The response from the IAM service indicating the status of the operation.

    Raises:
        Exception: If there is an error deleting the IAM role.
    """
    print(f"Deleting IAM role named '{iam_RoleName}'")
    iam.detach_role_policy(
        RoleName=iam_RoleName,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess",
    )
    response = iam.delete_role(RoleName=iam_RoleName)
    print(response)

    return response


def create_security_group(
    ec2_client, ec2_resource, group_name: str = "redshift_security_group"
):
    """
    Create a security group for Redshift cluster access.

    Args:
        ec2_client (boto3.client): The Boto3 EC2 client.
        ec2_resource (boto3.resource): The Boto3 EC2 resource.
        group_name (str, optional): The name for the security group. Defaults to "redshift_security_group".

    Returns:
        boto3.resources.factory.ec2.SecurityGroup: The created security group object.
    """
    try:
        response = ec2_client.create_security_group(
            Description="Authorise redshift cluster access",
            GroupName=group_name,
        )
        security_group_id = response["GroupId"]
        print(f"Security group {security_group_id} created successfully.")

        # Get the security group object
        security_group = ec2_resource.SecurityGroup(security_group_id)
        print(f"Security group object: {security_group}")
    except Exception as e:
        print(f"Error creating security group: {e}")
    return security_group


def get_public_ip():
    """
    Retrieve the public IP address of the local machine using the 'ifconfig.me' service.

    Returns:
        str: The public IP address as a string, or None if retrieval fails.
    """
    try:
        response = requests.get("https://ifconfig.me")
        response.raise_for_status()  # Raise an exception for non-2xx status codes
        return response.text.strip()
    except requests.exceptions.RequestException as e:
        print(f"Error getting public IP: {e}")
        return None


def create_redshift_cluster(
    redshift,
    roleArn,
    SecurityGroupID,
    cluster_type="multi-node",
    node_type="dc2.large",
    num_nodes=3,
):
    """
    Create a Redshift cluster.

    Args:
        redshift (boto3.client): The Boto3 Redshift client.
        roleArn (str): The ARN of the IAM role to associate with the cluster for S3 access.
        SecurityGroupID (str): The ID of the VPC security group to associate with the cluster.
        cluster_type (str, optional): The type of Redshift cluster. Defaults to "multi-node".
        node_type (str, optional): The node type for the cluster. Defaults to "dc2.large".
        num_nodes (int, optional): The number of nodes in the cluster. Defaults to 3.

    Returns:
        None
    """
    print("Creating Redshift Cluster")
    try:
        response = redshift.create_cluster(
            # HW
            ClusterType=cluster_type,
            NodeType=node_type,
            NumberOfNodes=num_nodes,
            # Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=CLUSTER_ID,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,
            VpcSecurityGroupIds=[SecurityGroupID],
            # Roles (for s3 access)
            IamRoles=[roleArn],
        )
    except Exception as e:
        print(e)


def delete_security_group(ec2_resource, group_name: str = "redshift_security_group"):
    """
    Delete a security group.

    Args:
        ec2_resource (boto3.resource): The Boto3 EC2 resource.
        group_name (str, optional): The name of the security group to delete. Defaults to "redshift_security_group".
    """
    try:
        # Describe security groups with the given name
        security_groups = list(
            ec2_resource.security_groups.filter(
                Filters=[{"Name": "group-name", "Values": [group_name]}]
            )
        )

        # Check if any security groups were found
        if security_groups:
            # Get the security group object from the first matching group
            security_group_obj = security_groups[0]
            print(f'Security group object for "{group_name}": {security_group_obj}')
            print(f"Deleting security group: '{group_name}'")
            response = security_group_obj.delete(
                GroupName=group_name,
            )
            print(response)
        else:
            print(f'No security group found with name "{group_name}"')

    except Exception as e:
        print(f"Error getting security group object: {e}")


def get_cluster_info(redshift):
    """
    Get the status and endpoint of the Redshift cluster.

    Args:
        redshift (boto3.client): The Boto3 Redshift client.

    Returns:
        tuple: A tuple containing the cluster status and endpoint address.
            - str: The status of the cluster if successful, otherwise None.
            - str: The endpoint address of the cluster if available, otherwise None.
    """
    try:
        # Describe the cluster
        response = redshift.describe_clusters(ClusterIdentifier="dwhCluster")

        # Get the cluster status from the response
        cluster_status = response["Clusters"][0]["ClusterStatus"]
        print(f"Cluster status: {cluster_status}")

        if cluster_status == "available":
            dwh_endpoint = response["Clusters"][0]["Endpoint"]["Address"]
            return cluster_status, dwh_endpoint

        return cluster_status, None

    except Exception as e:
        print(f"Error getting cluster status: {e}")
        return None, None


def delete_redshift_cluster(redshift, cluster_identifier):
    """
    Delete a Redshift cluster.

    Args:
        redshift (boto3.client): The Boto3 Redshift client.
        cluster_identifier (str): The identifier of the cluster to delete.

    Returns:
        None
    """
    try:
        response = redshift.delete_cluster(
            ClusterIdentifier=cluster_identifier, SkipFinalClusterSnapshot=True
        )
        print(f"Deleting cluster '{cluster_identifier}'...")
    except Exception as e:
        print(f"Error deleting cluster: {e}")


def init():
    """
    Initialize the setup process for Redshift.

    This function performs the following actions:
    - Creates an IAM Role for Redshift with S3 Read Access.
    - Creates a security group (firewall) for Redshift and allows connection from the local IP address.
    - Opens an incoming TCP port to access the cluster endpoint.
    - Creates a Redshift cluster.
    - Updates the configuration file with the IAM ARN.

    Raises:
        Exception: If retrieving the public IP address fails.
    """
    # Create IAM Role for Redshift with S3 Read Access
    iam_role = create_iam_role(iam)

    # Create security group (= firewall for Redshift) and allow connection from local IP
    sec_group = create_security_group(ec2_client, ec2_resource)
    public_ip = get_public_ip()
    if public_ip:
        print(f"Public IP address: {public_ip}")

        # Open Incoming TCP port to access the cluster endpoint
        sec_group.authorize_ingress(
            GroupName=sec_group.group_name,
            CidrIp=f"{public_ip}/32",  # Use /32 to specify a single IP address
            IpProtocol="TCP",
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT),
        )
        print("Open Incoming TCP port to current public IP address")
    else:
        raise Exception("Failed to retrieve public IP address")

    # Add more nodes to make it faster
    create_redshift_cluster(redshift, iam_role, sec_group.id, num_nodes=2)

    # Update the config file with the IAM ARN
    config["IAM_ROLE"]["ARN"] = iam_role
    with open(CONFIG_FILENAME, "w") as conf:
        config.write(conf)
        print(f"Updated {CONFIG_FILENAME} with IAM ARN")


def status():
    """
    Check the status of the Redshift cluster and update configuration accordingly.

    This function retrieves the status and endpoint of the Redshift cluster.
    If the cluster is available, it updates the configuration file with the
    cluster endpoint under the 'CLUSTER' section.

    """
    cluster_status, dwh_endpoint = get_cluster_info(redshift)

    if cluster_status == "available":
        print("You are ready to use your Redshift cluster!")

        # Update the config file with the DWH Endpoint
        config["CLUSTER"]["host"] = dwh_endpoint
        with open(CONFIG_FILENAME, "w") as conf:
            config.write(conf)
            print(f"Updated {CONFIG_FILENAME} with host")


def delete():
    """
    Perform cleanup by deleting IAM role, Redshift cluster, and associated security group.

    Returns:
        None
    """
    delete_iam_role(iam)
    delete_redshift_cluster(redshift, CLUSTER_ID)
    time.sleep(5)
    delete_security_group(ec2_resource)


if __name__ == "__main__":
    # Usage : python redshift_iac.py init/status/delete
    fire.Fire({"init": init, "status": status, "delete": delete})
