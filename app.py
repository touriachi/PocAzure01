from flask import Flask
import os, uuid, sys

import config
import azure.storage.common
from azure.storage.common import CloudStorageAccount
from queuetools import QueueTools
from blobstools import BlobsTools


app = Flask(__name__)


# Create the storage account object and specify its credentials
# to either point to the local Emulator or your Azure subscription
if config.IS_EMULATED:
    account = CloudStorageAccount(is_emulated=True)
else:
    account_name = config.STORAGE_ACCOUNT_NAME
    account_key = config.STORAGE_ACCOUNT_KEY
    account = CloudStorageAccount(account_name, account_key)

app=Flask(__name__)
@app.route('/')
def hello_world():
    return app.send_static_file('index2.html')



@app.route('/init')
def init_objects():
    try:
        print('Step 1 Initilisation dez objets')
        queue = QueueTools()
        queue.create_queue(account, config.INPUT_QUEUE)
        queue.create_queue(account, config.OUTPUT_QUEUE)
        print('Step 1.1 Queues  created with succes')
        blobs = BlobsTools()
        blobs.create_container(account, config.STORAGE_CONTAINER)
        print('Step 1.2 Container created with succes')
        return('Queues and  container are created with sucess!')
    except Exception as e:
        print(e)
        return('Error in  init_object method' , e)

@app.route('/setinput')
def set_input():
    try:
        queue = QueueTools()
        print('Step 2 On simule le depot du message dans la queue et du blob  par le UI ')
        queue.queue_message(account, config.INPUT_QUEUE, "PROCESS inputassets015")
        blobs = BlobsTools()
        blobs.save_jsonfile(account,config.STORAGE_CONTAINER, config.INPUT_FOLDER_NAME, config.JSON_FILE_NAME)
        return('Message and input json file added with  sucess!')

    except Exception as e:
        print(e)
        return ('Error in  set_data method ', e)

@app.route('/process')
def process_data():
    try:
        queue = QueueTools()
        messages_list = queue.dequeue_message(account, config.INPUT_QUEUE)
        blobs = BlobsTools()
        blobs.get_jsonfile(account, config.STORAGE_CONTAINER, config.LOCAL_PROCESSING_FOLDER_NAME, config.JSON_FILE_NAME)
        return('data getted with succes  and processed with sucess!')
        #start ALM process
        #.................................................
        #............................................
        #..........................................................................................
    except Exception as e:
        print(e)
        return ('Error in  process_data ', e)



@app.route('/pushresult')
def push_result():
    try:
        queue = QueueTools()
        queue.queue_message(account, config.OUTPUT_QUEUE, "COMPLETED inputassets015.json")

        blobs = BlobsTools()
        blobs.save_jsonfile(account, config.STORAGE_CONTAINER,config.LOCAL_PROCESSING_FOLDER_NAME, "inputassets015_result.json")

        return('json result  and queue  message  setted with with succes!')

    except Exception as e:
        print(e)
        return ('Error in  push_result method ', e)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=False)

