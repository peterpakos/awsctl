# cloud_tools
A tool to manipulate clouds

## AWS configuration
### AWS CLI
If you have the [AWS CLI](http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html) installed, then you can use its interactive configure command to set up your credentials:
```
$ aws configure
AWS Access Key ID [****************BLUQ]:
AWS Secret Access Key [****************0ECi]:
Default region name [eu-west-1]:
Default output format [json]:

```
### Manual setup
The tool reads AWS config and credentials from a local file named `config` and `credentials` in a folder named `.aws` in your home directory. Home directory location varies but can be referred to using the environment variables `%UserProfile%` in Windows and `$HOME` or `~` (tilde) in Unix-like systems.

To quickly start without installing AWS CLI, simply create the following files:

**~/.aws/credentials**
```
[default]
aws_access_key_id=****************BLUQ
aws_secret_access_key=****************0ECi
```

**~/.aws/config**
```
[default]
region=us-west-1
output=json
```

### Boto 3
The tool uses Python module Boto 3 to talk to AWS ([Boto 3 documentation](http://boto3.readthedocs.org/en/latest/guide/configuration.html)).

## Usage
```
$ ./cloud_tools -h
usage: cloud_tools [-h] [-v] [-c {aws,gce,azure}] [-p PROFILE_NAME] [-b] [-H]
                   [-r REGION]
                   [-s {running,pending,shutting-down,stopped,stopping,terminated}]
                   [{describe-instances,describe-regions}]

A tool to manipulate clouds.

positional arguments:
  {describe-instances,describe-regions}

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show version
  -c {aws,gce,azure}, --cloud-provider {aws,gce,azure}
                        cloud provider (default: aws)
  -p PROFILE_NAME, --profile-name PROFILE_NAME
                        cloud profile name (default: default)
  -b, --disable-border  disable table border
  -H, --disable-header  disable table header
  -r REGION, --region REGION
                        choose single region (default: all)
  -s {running,pending,shutting-down,stopped,stopping,terminated}, --state {running,pending,shutting-down,stopped,stopping,terminated}
                        display instances only in certain states
```
