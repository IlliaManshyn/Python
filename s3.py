import botocore
import boto3
#from boto.s3.key import Key
import botocore.vendored.requests.packages.urllib3 as urllib3
import argparse
import os
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

bucket_name = 'mybucket'
path_to_file = '/home/imanshyn/salt'
filename = 'salt'

# List of buckets
def list_of_buckets():
    s3Resource = boto3.resource(
     's3',
     endpoint_url=os.environ['AUTH_URL'],
     aws_access_key_id=os.environ['ACCESS_KEY'],
     aws_secret_access_key=os.environ['SECRET_KEY'],
     verify=False
    )
    print('Buckets:')
    i=1
    for bucket in s3Resource.buckets.all():
       print(f'\t{i}. {bucket.name}')
       i+=1

# Create bucket
def create_bucket(bucket_name):
    s3Client = boto3.client(
      's3',
       endpoint_url=os.environ['AUTH_URL'],
       aws_access_key_id=os.environ['ACCESS_KEY'],
       aws_secret_access_key=os.environ['SECRET_KEY'],
       verify=False
    )
    if bucket_name != None:
       s3Client.create_bucket(Bucket=bucket_name)
    else:
        print('Define bucket name. Use argument -b [Bucket name]')

# Delete bucket
def delete_bucket(bucket_name):
    s3Resource = boto3.resource(
     's3',
     endpoint_url=os.environ['AUTH_URL'],
     aws_access_key_id=os.environ['ACCESS_KEY'],
     aws_secret_access_key=os.environ['SECRET_KEY'],
     verify=False
    )
    if bucket_name != None:
      bucket=s3Resource.Bucket(bucket_name)
      for object in bucket.objects.all():
          object.delete()
      bucket.delete()
    else:
        print('Define bucket name. Use argument -b [Bucket name]')

# Delete file
def delete_file(bucket_name, objectname):
    s3Resource = boto3.resource(
     's3',
     endpoint_url=os.environ['AUTH_URL'],
     aws_access_key_id=os.environ['ACCESS_KEY'],
     aws_secret_access_key=os.environ['SECRET_KEY'],
     verify=False
    )
    if bucket_name != None and objectname != None:
        obj = s3Resource.Object(bucket_name, objectname)
        obj.delete()
    else:
        print('Not all parameters are set. Example: s3.py -orm -b [Bucket name] -n [File name]')

# List of files in bucket
def list_of_files(bucket_name):
   s3Resource = boto3.resource(
    's3',
    endpoint_url=os.environ['AUTH_URL'],
    aws_access_key_id=os.environ['ACCESS_KEY'],
    aws_secret_access_key=os.environ['SECRET_KEY'],
    verify=False
   )
   if bucket_name != None:
     i=1
     name = s3Resource.Bucket(bucket_name).name
     print(name + ":")
     for object in s3Resource.Bucket(bucket_name).objects.all():
        print(f'\t{i}. {object.key}')
        i += 1
   else:
      print('Define bucket name. Use argument -b [Bucket name]')

# Upload files to bucket
def upload_object_to_bucket(bucket_name, path_to_object, objectname):
    s3Client = boto3.client(
      's3',
       endpoint_url=os.environ['AUTH_URL'],
       aws_access_key_id=os.environ['ACCESS_KEY'],
       aws_secret_access_key=os.environ['SECRET_KEY'],
       verify=False
     )
    if bucket_name != None and path_to_file != None and objectname != None:
       s3Client.upload_file(path_to_object, bucket_name, objectname)
    else:
       print('Not all parameters are set. Example: s3.py -u -b [Bucket name] -p [Path to file] -n [File name]')

# Download file from bucket
def download_file(bucket_name, path_to_save , objectname):
   s3Resource = boto3.resource(
    's3',
    endpoint_url=os.environ['AUTH_URL'],
    aws_access_key_id=os.environ['ACCESS_KEY'],
    aws_secret_access_key=os.environ['SECRET_KEY'],
    verify=False
   )
   if bucket_name != None and objectname != None:
      try:
        s3Resource.Bucket(bucket_name).download_file(objectname, path_to_save)
      except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
          print("The object does not exist.")
        else:
          raise
   else:
       print('Not all parameters are set. Example: s3.py -dw -b [Bucket name] -n [File name] -s [Path to save]')


parser = argparse.ArgumentParser()
list = parser.add_mutually_exclusive_group()

parser.add_argument('-b', '--bucket_name', action="store")
parser.add_argument('-p', '--path_to_file', action="store")
parser.add_argument('-n', '--object_name', action="store")
parser.add_argument('-s', '--path_to_save', action="store")

list.add_argument('-cb', '--bucket_create',  help='Create bucket', action="store_true")
list.add_argument('-bls', '--bucket_list',  help='Show list of buckets', action="store_true")
list.add_argument('-brm', '--remove_bucket', help='Delete bucket, all objects will be deleted too', action="store_true")
list.add_argument('-u', '--upload_object',  help='Upload object to bucket', action="store_true")
list.add_argument('-ols', '--object_list',  help='Show list of objects', action="store_true")
list.add_argument('-orm', '--remove_object', help='Delete object', action="store_true")
list.add_argument('-dw', '--download_object', help='Download object', action="store_true")

args = parser.parse_args()

if args.bucket_create:
    create_bucket(args.bucket_name)
if args.remove_bucket:
    delete_bucket(args.bucket_name)
if args.bucket_list:
    list_of_buckets()


if args.upload_object:
    upload_object_to_bucket(args.bucket_name, args.path_to_file, args.object_name)
if args.object_list:
    list_of_files(args.bucket_name)
if args.remove_object:
    delete_file(args.bucket_name, args.object_name)
if args.download_object:
    download_file(args.bucket_name, args.path_to_save, args.object_name)
