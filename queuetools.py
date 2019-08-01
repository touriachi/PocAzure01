
from azure.storage.common import CloudStorageAccount
from azure.storage.queue import Queue, QueueService, QueueMessage
import config


class QueueTools():


    # Basic queue operations including creating and listing
    def create_queue(self, account, queuename):

        try:

            queue_service = account.create_queue_service()
            # Create a queue or leverage one if already exists
            print('Attempting create of queue: ', queuename)
            queue_service.create_queue(queuename)
            print('Successfully created queue: ', queuename)


        except Exception as e:
            if (config.IS_EMULATED):
                print('Error occurred in the sample. Please make sure the Storage emulator is running.', e)
            else:
                print('Error occurred in the sample. Please make sure the account name and key are correct.', e)

    def queue_message(self, account,queuename, messagename):
        queue_service = account.create_queue_service()
        queue_service.put_message(queuename, messagename)
        print('Successfully added message: ', messagename)

    def dequeue_message(self, account, queuename):
        # Dequeuing a message
        # First get the message, to read and process it.
        #  Specify num_messages to process a number of messages. If not specified, num_messages defaults to 1
        #  Specify visibility_timeout optionally to set how long the message is visible
        queue_service = account.create_queue_service()
        messages = queue_service.get_messages(queuename)
        messages_list =[]
        for message in messages:
            print('Message for dequeueing is: ', message.content)
            # Then delete it.
            # When queue is deleted all messages are deleted, here is done for demo purposes
            # Deleting requires the message id and pop receipt (returned by get_messages)


            messages_list.append(message.content)
            queue_service.delete_message(queuename, message.id, message.pop_receipt)
            print('Successfully dequeued message')
        return  messages_list

    # Delete the queue
    def delete_queue(self, account, queuename):
        # Delete the queue. 
        # Warning: This will delete all the messages that are contained in it.
        queue_service = account.create_queue_service()
        print('Attempting delete of queue: ', queuename)
        if queue_service.exists(queuename):
            queue_service.delete_queue(queuename)    
            print('Successfully deleted queue: ', queuename)




