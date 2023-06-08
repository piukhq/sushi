import click

from sushi.main import sftpTransfer


@click.group()
def cli():
    pass


@cli.command(name="upload")
@click.option("-h", "--host", help="SFTP host", type=str)
@click.option("-p", "--port", help="SFTP port", type=int)
@click.option("-u", "--username", help="SFTP username", type=str)
@click.option("-kn", "--key-name", help="Key Name", type=str)
@click.option("-cs", "--chunk-size", help="Chunk Size", type=int)
def upload(username, host, port, key_name, chunk_size):
    s = sftpTransfer(host=host, username=username, port=port, key_name=key_name, chunk_size=chunk_size)
    s.run()

