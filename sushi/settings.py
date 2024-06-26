from pydantic import BaseSettings


class Settings(BaseSettings):
    vault_url: str = "https://tv-test.vault.azure.net/"
    sftp_host: str = "sftp.gb.bink.com"
    sftp_port: int = 22
    sftp_username: str = "binktest_dev"
    sftp_dir: str = "Live Feed"
    key_name: str = "wasabi-sftp"
    chunk_size: int = 500
    import_container: str = "harmonia-imports"
    archive_container: str = "harmonia-archive"


settings = Settings()
