# cloud_tools
Tool to operate clouds

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
$ cloud_tools --help
usage: cloud_tools [-h] [-v] [--debug] [--verbose] [-c {aws,gcp,azure}]
                   [-p PROFILE_NAME]
                   {list,list-regions,list-hdi,exclude,include,tag,sg,public-buckets,run,create-image,stop,terminate,start}
                   ...

Tool to operate clouds

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  --debug               debugging mode
  --verbose             verbose debugging mode
  -c {aws,gcp,azure}, --cloud-provider {aws,gcp,azure}
                        cloud provider (default: aws)
  -p PROFILE_NAME, --profile-name PROFILE_NAME
                        cloud profile name (default: infra)

commands:
  {list,list-regions,list-hdi,exclude,include,tag,sg,public-buckets,run,create-image,stop,terminate,start}
    list                display list of instances
    list-regions        display list of available regions
    list-hdi            display list of HDI clusters
    exclude             exclude instances from alerting (create EXCLUDE tag)
    include             include instances in alerting (delete EXCLUDE tag)
    tag                 tag instances
    sg                  batch edit Security Groups rules
    public-buckets      check public buckets
    run                 run instances
    create-image        create image
    stop                stop instances
    terminate           terminate instances
    start               start instances

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
Record AWS API calls into S3 bucket.

    Trail name: auto-tag
    Apply trail to all regions: Yes
    Create a new S3 bucket: Yes
    S3 bucket: bucket-name
