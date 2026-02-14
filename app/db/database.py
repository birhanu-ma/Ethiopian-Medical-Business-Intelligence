from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core.config import settings # Assuming DATABASE_URL is in your config

engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()