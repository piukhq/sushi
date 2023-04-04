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
    def __init__(self):
        self.secret = settings.config
        self.vault_url = settings.vault_url
    
    def vault_client():
        vault_credential = DefaultAzureCredential()
        vault_client = SecretClient(vault_url=settings.vault_url, credential=vault_credential)
        return vault_client.get_secret("wasabi-sftp").value

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
        s = self.sftp_client(host="178.128.35.177", port=22, username="wasabi")
        sftp_dir = "upload"
        chunk = None
        with s.open(f"{sftp_dir}/{file}", "r") as original:
            for line_no, line in enumerate(original):
                if line_no % 5 == 0:
                    if chunk:
                        chunk.close()
                    chunk_name = f"{file}_chunk_{line_no}.csv"
                    chunk = s.open(f"chunks/{chunk_name}", "w+")
                chunk.write(line)
            if chunk:
                chunk.close()

    def upload_blob(self, container: str, filename: str, data: BytesIO) -> None:
        client = BlobServiceClient.from_connection_string(self.get_storage_key())
        blob = client.get_blob_client(container=container, blob=filename)
        try:
            blob.upload_blob(data)
        except ResourceExistsError:
            print(f"{filename} already exists in this import directory.")

    def archive_sftp_file(self) -> None:
        sftp_dir = "upload"
        s = self.sftp_client(host="178.128.35.177", port=22, username="wasabi")
        try:
            for time in ["YYYY", "YYYY/MM", "YYYY/MM/DD"]:
                s.mkdir(f"/archive/{pendulum.today().format(time)}")
        except IOError:
            pass
        for i in s.listdir(sftp_dir):
            s.rename(f"/upload/{i}", f"/archive/{pendulum.today().format('YYYY/MM/DD')}/{i}")

    def archive_blob(self, filename: str, data: bytes) -> None:
        client = BlobServiceClient.from_connection_string(self.get_storage_key())
        archive_container = "harmonia-archive"
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
        s = self.sftp_client()
        sftp_dir = "upload"
        blob_container = "harmonia-imports"
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