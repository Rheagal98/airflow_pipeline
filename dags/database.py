from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config import setting

SQLALCHEMY_DATABASE_URL = f"postgresql://{setting.DB_USER}:{setting.DB_PASSWORD}@{setting.DB_HOST}:{setting.DB_PORT}/{setting.DB_NAME}"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()
