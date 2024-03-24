from pydantic_settings import BaseSettings, SettingsConfigDict


class Setting(BaseSettings):
    SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    DB_HOST: str = 'postgres'
    DB_PORT: str = '5432'
    DB_USER: str = 'airflow'
    DB_PASSWORD: str = 'airflow'
    DB_NAME: str = 'trip'


setting = Setting()
