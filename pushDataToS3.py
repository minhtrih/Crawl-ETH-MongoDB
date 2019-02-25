import boto3
import csv
import json
import sys
from os import path, remove

start_block = 35
end_block = 35
path_folder = 'output/blocks/'
csv.field_size_limit(sys.maxsize)
while(True):
    name_file = path_folder + 'start_block=0'+str(start_block)+'00000/end_block=0'+str(
        end_block)+'99999/blocks_0'+str(start_block)+'00000_0'+str(end_block)+'99999.csv'
    if path.exists(name_file) is True:
        print('converting to json')
        csvfile = open(name_file, 'r')
        name_jsonfile = 'blocks_0' + \
            str(start_block)+'00000_0'+str(end_block)+'99999.json'
        jsonfile = open(name_jsonfile, 'w')
        # fieldnames = ("hash", "nonce", "block_hash", "block_number", "transaction_index",
        #           "from_address", "to_address", "value", "gas", "gas_price", "input")
        fieldnames = ("number", "hash", "parent_hash", "sha3_uncles", "logs_bloom", "transactions_root",
                      "state_root", "receipts_root", "miner", "difficulty", "total_difficulty", "size", "extra_data", "gas_limit", "gas_used", "timestamp", "transaction_count")
        reader = csv.DictReader(csvfile, fieldnames)
        for row in reader:
            json.dump(row, jsonfile)
            jsonfile.write('\n')
        s3 = boto3.resource('s3')
        s3.meta.client.upload_file(name_jsonfile,
                                   'bk-cypto-data', name_jsonfile)
        s3.meta.client.upload_file(name_file,
                                   'bk-cypto-data', name_file)
        print('complete upload_file ' + name_jsonfile)
        start_block += 1
        end_block += 1
        # remove(name_jsonfile)
