from azure.storage.blob import BlockBlobService, PublicAccess
import os, uuid, sys
import config


class BlobsTools():
    # Basic bolobs operations
    def create_container(self, account, container_name):
        try:
                # Create the BlockBlockService that is used to call the Blob service for the storage account
                block_blob_service = BlockBlobService(account.account_name,account.account_key)

                # Create a container
                block_blob_service.create_container(container_name)

        except Exception as e:
                print('Error occurred in create container.', e)

    def save_jsonfile(self, account, container_name,folder_name,local_file_name):
        try:
            block_blob_service = BlockBlobService(account.account_name, account.account_key)
            # Set the permission so the blobs are public.
            block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)

            # Create a file in Documents to test the upload and download.
            full_path_to_file = os.path.join(folder_name, local_file_name)

            #Upload the created file, use local_file_name for the blob name
            block_blob_service.create_blob_from_path(container_name, local_file_name, full_path_to_file)

            # List the blobs in the container
            print("\nList blobs in the container")
            generator = block_blob_service.list_blobs(container_name)
            for blob in generator:
                print("\t Blob name: " + blob.name)

            sys.stdout.write("Don't forget to delete resources "
                             "application will exit.")
            sys.stdout.flush()

        except Exception as e:
            print('Error occurred in create container.', e)

    def get_jsonfile(self, account, container_name,folder_name, local_file_name):
        try:

            block_blob_service = BlockBlobService(account.account_name, account.account_key)
            # Set the permission so the blobs are public.
            block_blob_service.set_container_acl(container_name, public_access=PublicAccess.Container)
            # Download the blob(s).
            full_path_to_file = os.path.join(folder_name, local_file_name)
            block_blob_service.get_blob_to_path(container_name, local_file_name, full_path_to_file)
        except Exception as e:
            print('Error occurred in get_jsonfile.', e)
