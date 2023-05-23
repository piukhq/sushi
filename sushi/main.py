import csv
import json
import paramiko
import pendulum

from io import BytesIO, StringIO
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceExistsError

from sushi.settings import settings


class sftpTransfer:
    def __init__(self, host, port, username, chunk_size, key_name, import_container = None, archive_container = None, dir = None):
        self.vault_url = settings.vault_url
        self.dir = dir if dir else settings.sftp_dir
        self.host = host if host else settings.sftp_host
        self.port = port if port else settings.sftp_port
        self.key_name = key_name if key_name else settings.key_name
        self.username = username if username else settings.sftp_username
        self.chunk_size = chunk_size if chunk_size else settings.chunk_size
        self.import_container = import_container if import_container else settings.import_container
        self.archive_container = archive_container if archive_container else settings.archive_container


    def vault_client(self):
        vault_credential = DefaultAzureCredential()
        vault_client = SecretClient(vault_url=settings.vault_url, credential=vault_credential)
        return vault_client.get_secret(self.key_name).value

    def get_sftp_key(self) -> StringIO:
        key = json.loads(self.vault_client())["key"]
        return StringIO(key)

    def get_storage_key(self) -> str:
        return json.loads(self.vault_client())["connection_string"]

    def sftp_client(self, host: str, port: int, username: str) -> paramiko.SFTPClient:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        key = paramiko.RSAKey.from_private_key(self.get_sftp_key())
        ssh.connect(host, port=port, username=username, pkey=key)
        return ssh.open_sftp()

    def split_file(self, file) -> None:
        s = self.sftp_client(host=self.host, port=self.port, username=self.username)
        sftp_dir = self.dir
        chunk_size = self.chunk_size
        output = None
        with s.open(f"{sftp_dir}/{file}", "r") as original:
            reader = csv.DictReader(original)
            for counter, row in enumerate(iter(reader)):
                headers = row.keys()
                if counter % chunk_size == 0:
                    print("splitting part {} of {}".format(counter, file))
                    if output is not None:
                        output.close()
                    chunk_name = f"{file}_chunk_{counter}.csv"
                    output = s.open(f"chunks/{chunk_name}", "w+")
                    dict_writer = csv.DictWriter(output, fieldnames=headers, delimiter=",")
                    dict_writer.writeheader()
                dict_writer.writerow(row)

    def upload_blob(self, container: str, filename: str, data: BytesIO) -> None:
        client = BlobServiceClient.from_connection_string(self.get_storage_key())
        blob = client.get_blob_client(container=container, blob=filename)
        try:
            blob.upload_blob(data)
        except ResourceExistsError:
            print(f"{filename} already exists in this import directory.")

    def archive_sftp_file(self) -> None:
        sftp_dir = self.dir
        s = self.sftp_client(host=self.host, port=self.port, username=self.username)
        for time in ["YYYY", "YYYY/MM", "YYYY/MM/DD"]:
            try:
                directory = pendulum.today().format(time)
                s.mkdir(f"archive/{directory}")
            except IOError as e:
                continue
        for i in s.listdir(sftp_dir):
            s.rename(f"/upload/{i}", f"/archive/{pendulum.today().format('YYYY/MM/DD')}/{i}")

    def archive_blob(self, filename: str, data: bytes) -> None:
        client = BlobServiceClient.from_connection_string(self.get_storage_key())
        archive_container = self.archive_container
        try:
            client.create_container(archive_container)
        except ResourceExistsError:
            pass
        try:
            client.get_blob_client(
                archive_container, f"{pendulum.today().format('YYYY/MM/DD')}/{filename}"
            ).upload_blob(data)
        except ResourceExistsError:
            print(f"{filename} already exists in this archive directory.")

    def delete_blob(self) -> None:
        client = BlobServiceClient.from_connection_string(self.get_storage_key())
        container_client = client.get_container_client(container="harmonia-imports")
        blob_list = container_client.list_blobs()
        for i in blob_list:
            container_client.delete_blob(i)

    def run(self):
        s = self.sftp_client(host=self.host, port=self.port, username=self.username)
        sftp_dir = self.dir
        blob_container = self.import_container
        for i in s.listdir(sftp_dir):
            data = BytesIO()
            s.getfo(f"{sftp_dir}/{i}", data)
            data.seek(0)
            self.archive_blob(filename=i, data=data)
            self.split_file(i)

        for i in s.listdir('chunks/'):
            data = BytesIO()
            s.getfo(f"chunks/{i}", data)
            data.seek(0)
            self.upload_blob(container=blob_container, filename=i, data=data)
            s.remove(f"chunks/{i}")

        self.archive_sftp_file()