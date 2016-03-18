# cloud_master
A tool to manipulate clouds

## Usage
```
$ ./cloud_master.py list-instances -h
usage: cloud_master.py [-h] [-v] [-c {aws}] [-p PROFILE_NAME] [-b] [-H]
                       [-r REGION]
                       [-s {running,pending,shutting-down,stopped,stopping,terminated}]
                       [{list-instances,list-regions}]

A tool to manipulate clouds.

positional arguments:
  {list-instances,list-regions}

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show version
  -c {aws}, --cloud-provider {aws}
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
