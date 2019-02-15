import pymongo
import json
from web3 import Web3, HTTPProvider
from hexbytes import HexBytes
import os.path
from os import path

file_count = 0

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

currentBlock = web3.eth.blockNumber
# lastBlock = 7188162
for block in range(currentBlock):
    block_crawl = currentBlock-block
    print(block_crawl)
    blockTransactionCount = web3.eth.getBlockTransactionCount(
        block_crawl)
    val_arr = []
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
            'value': "%.18f" % float(result['value']/1000000000000000000)
        }
        val_arr.append(val_obj)
    if len(val_arr) > 0:
        if block%2000 == 0:
            file_count += 1
            file_name = 'data'+str(file_count)+'.json'
            if path.exists(file_name) is True:
                with open(file_name, 'r') as fp:
                    information = json.load(fp)
                    information = information + val_arr
                with open(file_name, 'w') as fp:
                    json.dump(information, fp, indent=2)
                val_arr = None
                information = None
            else:
                with open(file_name, 'w') as fp:
                    json.dump(val_arr, fp, indent=2)
                val_arr = None
        else: 
            file_name = 'data'+str(file_count)+'.json'
            if path.exists(file_name) is True:
                with open(file_name, 'r') as fp:
                    information = json.load(fp)
                    information = information + val_arr
                with open(file_name, 'w') as fp:
                    json.dump(information, fp, indent=2)
                val_arr = None
                information = None
            else:
                with open(file_name, 'w') as fp:
                    json.dump(val_arr, fp, indent=2)
                val_arr = None
