# aws-s3-share

![PyPI - Version](https://img.shields.io/pypi/v/aws-s3-share) ![PyPI - Python Version](https://img.shields.io/pypi/pyversions/aws-s3-share) ![PyPI - License](https://img.shields.io/pypi/l/aws-s3-share)

A command-line tool for compressing files or directories, uploading them to Amazon S3, then generating a pre-signed URL for easy sharing.  This is useful for example for uploading/downloading files from EC2 instances without direct connexion to the Internet.

Compressed archives are uploaded with a random 12-characters long prefix to avoid overwriting of files of the same name.

## Features

- **Compression**: .gz for files, tar.gz for directories
- **Multipart S3 Upload**: Efficient uploads to Amazon S3
- **Pre-signed URLs**: Generate pre-signed URLs with configurable expiry
- **Progress Tracking**: Progress bars for compression and upload
- **AWS Profile Support**: Use AWS profiles for authentication
- **OS Independent**: Works on Linux, macOS, and Windows

## Installation

### Using uv (recommended)

```bash
uvx aws-s3-share
```

### Using pipx

```bash
pipx install aws-s3-share
```

### Using pip

```bash
pip install aws-s3-share
```

## Quick Start

### Basic Usage

```bash
# Upload a file
aws-s3-share --bucket my-bucket myfile.txt

# Upload a directory
aws-s3-share --bucket my-bucket mydirectory/

# Specify expiry time (in seconds)
aws-s3-share  --bucket my-bucket --expiry 7200 myfile.txt

# Specify an AWS profile
aws-s3-share --bucket my-bucket --profile myprofile myfile.txt
```

## Configuration

aws-s3-share supports command-line arguments and reading its options from a configuration file (`~/.config/aws-s3-share.toml` on Linux/macOS, `%AppData%\Roaming\aws-s3-share.toml` on Windows).  Valid configuration file options are `bucket`, `expiry`, and `profile`.

Options are applied in this order of precedence:

1. Command-line arguments (highest priority)
2. Configuration file options
3. Default values (lowest priority, expiry only)

### Command-line Options

```bash
aws-s3-share [OPTIONS] PATH

Options:
  -b, --bucket TEXT     S3 bucket to upload to
  -e, --expiry INTEGER  Pre-signed URL expiry time in seconds (default: 3600)
  -p, --profile TEXT    AWS profile name to use for authentication
  -h, --help           Show this message and exit
  --version            Show the version and exit
```

### AWS Credentials

If neither the command-line argument `--profile` nor the configuration file option `profile` is provided, aws-s3-share uses the standard AWS credential chain:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. AWS credentials file (`~/.aws/credentials`)
3. IAM roles (when running on EC2)

## Required IAM Permissions


```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::your-bucket-name/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:CreateMultipartUpload",
                "s3:CompleteMultipartUpload",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploads",
                "s3:ListParts"
            ],
            "Resource": "arn:aws:s3:::your-bucket-name/*"
        }
    ]
}
```

