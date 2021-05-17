import os, uuid, sys

from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from dotenv import load_dotenv

load_dotenv()


def initialize_storage_account(storage_account_name=os.environ.get('Name'),
                               storage_account_key=os.environ.get('Key')):
    try:
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)

    except Exception as e:
        print(e)


# def initialize_storage_account_ad(storage_account_name, client_id, client_secret, tenant_id):
#
#     try:
#         global service_client
#
#         credential = ClientSecretCredential(tenant_id, client_id, client_secret)
#
#         service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
#             "https", storage_account_name), credential=credential)
#
#     except Exception as e:
#         print(e)


def download_file_from_directory(fs, directory, fn):
    try:
        file_system_client = service_client.get_file_system_client(file_system=fs)

        directory_client = file_system_client.get_directory_client(directory)

        local_file = open(f"data/{fn}", 'wb')

        file_client = directory_client.get_file_client(fn)

        download = file_client.download_file()

        downloaded_bytes = download.readall()

        local_file.write(downloaded_bytes)

        local_file.close()

    except Exception as e:
        print(e)


def download_all_data():
    initialize_storage_account()
    files = ['stats.csv', 'computations.csv', 'subject-info.csv']
    fs = 'responsesdatasets'
    directory = 'participants'
    for fn in files:
        download_file_from_directory(fs, directory, fn)


if __name__ == '__main__':
    print("Downloading...")
    download_all_data()
    print("Done.")
