import csv
import json
from io import StringIO

import paramiko
import pendulum
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from sushi.settings import settings


class sftpTransfer:
    def __init__(
        self, host, port, username, chunk_size, key_name, import_container=None, archive_container=None, dir=None
    ):
        self.vault_url = settings.vault_url
        self.dir = dir if dir else settings.sftp_dir
        self.host = host if host else settings.sftp_host
        self.port = port if port else settings.sftp_port
        self.key_name = key_name if key_name else settings.key_name
        self.username = username if username else settings.sftp_username
        self.chunk_size = chunk_size if chunk_size else settings.chunk_size
        self.import_container = import_container if import_container else settings.import_container

    def vault_client(self):
        vault_credential = DefaultAzureCredential()
        vault_client = SecretClient(vault_url=settings.vault_url, credential=vault_credential)
        return vault_client.get_secret(self.key_name).value

    def get_sftp_key(self) -> StringIO:
        key = json.loads(self.vault_client())["key"]
        return StringIO(key)

    def sftp_client(self) -> paramiko.SFTPClient:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        key = paramiko.RSAKey.from_private_key(self.get_sftp_key())
        ssh.connect(self.host, port=self.port, username=self.username, pkey=key)
        return ssh.open_sftp()

    def split_file(self, file) -> None:
        s = self.sftp_client()
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

    def archive_sftp_file(self) -> None:
        sftp_dir = self.dir
        s = self.sftp_client()
        for time in ["YYYY", "YYYY/MM", "YYYY/MM/DD"]:
            try:
                directory = pendulum.today().format(time)
                s.mkdir(f"archive/{directory}")
            except IOError:
                continue
        for i in s.listdir(sftp_dir):
            s.rename(f"/upload/{i}", f"/archive/{pendulum.today().format('YYYY/MM/DD')}/{i}")

    def run(self):
        s = self.sftp_client()
        for i in s.listdir(self.dir):
            self.split_file(i)

        for i in s.listdir("chunks/"):
            s.get(f"chunks/{i}", f"/mnt/{self.import_container}/scheme/wasabi/{i}")
            s.remove(f"chunks/{i}")

        self.archive_sftp_file()
