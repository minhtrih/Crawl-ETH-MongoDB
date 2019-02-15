from blockchain import blockexplorer, statistics
import ssl
import json
import pymongo

myclient = pymongo.MongoClient(
    "mongodb+srv://minhtrih:minhtrih@cluster0-5rhqc.mongodb.net/test?retryWrites=true&ssl=true&ssl_cert_reqs=CERT_NONE")
mydb = myclient["btc_data"]
btc_data = mydb.btc_data
ssl._create_default_https_context = ssl._create_unverified_context
latest_block = blockexplorer.get_latest_block()
latest_block_json = latest_block.__dict__
height = latest_block_json['height']
for block_height in range(1):
    blocks = blockexplorer.get_block_height(height-block_height)
    transaction_hash = ''
    if blocks and len(blocks) == 1:
        blocks_json = blocks[0].__dict__
        if blocks_json['transactions'] and blocks_json['transactions']:
            transactions = blocks_json['transactions']
            value_transactions = []
            for tx in range(len(transactions)):
                transaction_number = 'transaction ' + str(tx)
                value_inputs = []
                value_outputs = []
                transaction = transactions[tx].__dict__
                transaction_hash = transaction['hash']
                if transaction['inputs'] and len(transaction['inputs']):
                    for ip in range(len(transaction['inputs'])):
                        input = transaction['inputs'][ip].__dict__
                        if 'address' in input:
                            value_input = {
                                'address_from': input['address'],
                                'value': "%.8f" % float(input['value']/100000000)
                            }
                            value_inputs.append(value_input)
                            if transaction['outputs'] and len(transaction['outputs']):
                                for op in range(len(transaction['outputs'])):
                                    output = transaction['outputs'][op].__dict__
                                    if 'address' in output:
                                        value_output = {
                                            'address_to': output['address'],
                                            'value': "%.8f" % float(output['value']/100000000)
                                        }
                                        value_outputs.append(value_output)
                value_transaction = {
                    transaction_number: [
                        {'hash': transaction_hash, 'inputs': value_inputs, 'outputs': value_outputs}]
                }
                value_inputs = None
                value_outputs = None
                value_transactions.append(value_transaction)
            value = {
                'hash': blocks_json['hash'],
                'block_height': blocks_json['height'],
                'transactions': value_transactions
            }
            value_transactions = None
            btc_data.insert_one(value)
            value = None
