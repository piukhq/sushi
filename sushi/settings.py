from pydantic import BaseSettings


class Settings(BaseSettings):
    vault_url: str = "https://tv-test.vault.azure.net/"
    config: dict = {"wasabi": {"sftp_keyname": "wasabi-sftp", "storage_keyname": "harmonia-imports"}}


settings = Settings()
