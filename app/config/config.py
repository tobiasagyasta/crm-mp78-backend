import os
from datetime import timedelta

class Config:
    SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/crm_mp78_db')
    JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY', 'your-secret-key')
    JWT_ACCESS_TOKEN_EXPIRES = timedelta(hours=1)
