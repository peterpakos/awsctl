# cloud_tools
A tool to manipulate clouds

## AWS configuration
### AWS CLI
If you have the [AWS CLI](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html) installed, then you can use its interactive configure command to set up your credentials:

    $ aws configure
    AWS Access Key ID [****************BLUQ]:
    AWS Secret Access Key [****************0ECi]:
    Default region name [eu-west-1]:
    Default output format [json]:


### Manual setup
The tool reads AWS config and credentials from a local file named `config` and `credentials` in a folder named `.aws` in your home directory. Home directory location varies but can be referred to using the environment variables `%UserProfile%` in Windows and `$HOME` or `~` (tilde) in Unix-like systems.

To quickly start without installing AWS CLI, simply create the following files:

**~/.aws/credentials**

    [default]
    aws_access_key_id=****************BLUQ
    aws_secret_access_key=****************0ECi


**~/.aws/config**

    [default]
    region=us-west-1
    output=json


### Boto 3
The tool uses Python module Boto 3 to talk to AWS ([Boto 3 documentation](http://boto3.readthedocs.org/en/latest/guide/configuration.html)).

## Usage

~~~
$ ./cloud_tools -h
usage: cloud_tools [-h] [--version] [-c {aws,gce,azure}] [-p PROFILE_NAME]
                   [-r REGION]
                   {describe-instances,describe-regions,exclude,include,tag}
                   ...

A tool to manipulate clouds

optional arguments:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  -c {aws,gce,azure}, --cloud-provider {aws,gce,azure}
                        cloud provider (default: aws)
  -p PROFILE_NAME, --profile-name PROFILE_NAME
                        cloud profile name (default: default)
  -r REGION, --region REGION
                        choose single region (default: all)

commands:
  {describe-instances,describe-regions,exclude,include,tag}
    describe-instances  display list of instances
    describe-regions    display list of available regions
    exclude             exclude instances from alerting (create EXCLUDE tag)
    include             include instances in alerting (delete EXCLUDE tag)
    tag                 tag instances
~~~

## AWS Lambda function
The included lambda funcion automatically adds `Last_user` tag to EC2 instances.

### Code
Either edit code inline or upload a .zip file.

### Configuration
Runtime: Python2.7
Handler: lambda_function.lambda_handler
Role: LambdaAutoTagRole
Description: Auto tag resources
Memory (MB): 128
Timeout: 0 min 30 sec
VPC: No VPC

### Event sources
Event source type: S3
Bucket: bucket-name
Event type: Object Created (All)

### LambdaAutoTagRole Policy:
~~~
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:*",
                "ec2:*",
                "autoscaling:*",
                "elasticmapreduce:*"
            ],
            "Resource": [
                "*"
            ]
        }
    ]
}
~~~

### AWS CloudTrail
Trail name: auto-tag
Apply trail to all regions: Yes
Create a new S3 bucket: Yes
S3 bucket: bucket-name
