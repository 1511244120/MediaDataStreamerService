from sqlalchemy import Column, Integer, Text
from database.models.base import Base

class Users(Base):
    __tablename__ = 'users'
    
    user_id = Column(Integer, primary_key=True)
    user_name = Column(Text, nullable=False)
    gender = Column(Text, nullable=False, unique=True)
    age = Column(Integer, nullable=False, unique=True)