import boto3

file_name = 'transactions_07000000_07099999.csv'

s3 = boto3.resource('s3')
s3.meta.client.upload_file(file_name, 'bk-cypto-data', file_name)