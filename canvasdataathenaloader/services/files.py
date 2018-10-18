import boto3
import requests


def download_file_from_url(url, dest):
    local_filename = "{}/{}".format(dest, url.split('/')[-1])
    request = requests.get(url, stream=True)

    file = open(local_filename, 'wb')
    for chunk in request.iter_content(chunk_size=1024):
        if chunk:
            file.write(chunk)
    file.close()
    return local_filename


def upload_file_to_s3(bucket, filename, prefix):
    s3 = boto3.client('s3')
    object_key = "{}/{}".format(prefix, filename.split('/')[-1])
    file = open(filename)
    s3.put_object(Bucket=bucket, Key=object_key, Body=file)
    file.close()
