from sqlalchemy import Column, Integer, ForeignKey, DateTime
from database_ops.models.base import Base

class TrackStreamEventLog(Base):
    __tablename__ = 'track_stream_event_log'
    
    user_id = Column(Integer, ForeignKey('users.user_id'), primary_key=True)
    track_id = Column(Integer, ForeignKey('tracks.track_id'), primary_key=True)
    event_timestamp = Column(DateTime, nullable=False)
    created_timestamp = Column(DateTime, nullable=False)