import pymongo
import json
from web3 import Web3, HTTPProvider
from hexbytes import HexBytes
import os.path
import sys
from os import path, remove
from datetime import datetime
import boto3

class HexJsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, HexBytes):
            return obj.hex()
        return super().default(obj)

# myclient = pymongo.MongoClient(
#     "mongodb+srv://minhtrih:minhtrih@cluster0-5rhqc.mongodb.net/test?retryWrites=true&ssl=true&ssl_cert_reqs=CERT_NONE")
# mydb = myclient["eth_data"]
# eth_data = mydb.eth_data

web3 = Web3(HTTPProvider("https://mainnet.infura.io"))
if path.exists('index.json') is True:
    with open('index.json', 'r') as fp:
        index = json.load(fp)  
        currentBlock = index['continueBlock']
        file_count = index['file_count']
        file_name = 'eth_data'+str(file_count)+'.json'
else: 
    currentBlock = web3.eth.blockNumber
    file_count = 0
    file_name = 'eth_data'+str(file_count)+'.json'
val_arr = []
for block in range(100000):
    block_crawl = currentBlock-block
    print(block_crawl)
    blockTransactionCount = web3.eth.getBlockTransactionCount(
        block_crawl)
    if blockTransactionCount > 0:
      get_block = web3.eth.getBlock(block_crawl)
      get_block = dict(get_block)
      get_block_json = json.dumps(get_block, cls=HexJsonEncoder)
      info_block = json.loads(get_block_json)
      timestamp_block = info_block['timestamp']
      if timestamp_block is not None: 
          date_time_block = datetime.fromtimestamp(timestamp_block)
      for transaction in range(blockTransactionCount):
          tx = web3.eth.getTransactionFromBlock(block_crawl, transaction)
          tx_dict = dict(tx)
          tx_json = json.dumps(tx_dict, cls=HexJsonEncoder)
          result = json.loads(tx_json)
          val_obj = {
              'blockNumber': int(result['blockNumber']),
              'hash': result['hash'],
              'from': result['from'],
              'to': result['to'],
              'gasPrice': "%.18f" % float(result['gasPrice']/1000000000000000000),
              'value': "%.18f" % float(result['value']/1000000000000000000),
              'timestamp': str(date_time_block)
          }
          val_arr.append(val_obj)
      if len(val_arr) > 0:
          if sys.getsizeof(val_arr) > 60000000:
              file_count += 1
              file_name = 'eth_data'+str(file_count)+'.json'
              while path.exists(file_name) is True:
                  file_count += 1
                  file_name = 'eth_data'+str(file_count)+'.json'
              with open(file_name, 'w') as fp:
                  json.dump(val_arr, fp, indent=2)
              val_arr = []
              s3 = boto3.resource('s3')
              s3.meta.client.upload_file(file_name, 'bk-cypto-data', file_name)
              file_delete = 'eth_data'+str(file_count-2)+'.json'
              if path.exists(file_delete) is True:
                  remove(file_delete)
          # else:
          #     file_name = 'eth_data'+str(file_count)+'.json'
          #     if path.exists(file_name) and path.getsize(file_name) > 60000000:
          #         file_count += 1
          #         file_name = 'eth_data'+str(file_count)+'.json'
          #         while path.exists(file_name) is True:
          #             file_count += 1
          #             file_name = 'eth_data'+str(file_count)+'.json'
          #     with open(file_name, 'w') as fp:
          #         json.dump(val_arr, fp, indent=2)
          #     val_arr = []
      with open('index.json', 'w') as fp:
          index = {
              "continueBlock": block_crawl,
              "file_count": file_count
          }
          json.dump(index, fp, indent=2)
        # else: 
        #     file_name = 'data'+str(file_count)+'.json'
        #     if path.exists(file_name) is True:
        #         with open(file_name, 'r') as fp:
        #             information = json.load(fp)
        #             information = information + val_arr
        #         with open(file_name, 'w') as fp:
        #             json.dump(information, fp, indent=2)
        #         val_arr = None
        #         information = None
        #     else:
        #         with open(file_name, 'w') as fp:
        #             json.dump(val_arr, fp, indent=2)
        #         val_arr = None
