import click

from sushi.main import sftpTransfer
from io import BytesIO


@click.group()
def cli():
    pass


@cli.command(name="upload")
def upload():
    s = sftpTransfer()
    s.run()


@cli.command(name="archive")
def archive():
    s = sftpTransfer()
    s.archive_sftp_file()
    s.delete_blob()


@cli.command(name="split")
def split():
    s = sftpTransfer()
    s.split_file()
