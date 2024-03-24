from database import Base, engine
from sqlalchemy import Column, Integer, String, DateTime, Float, inspect


class Trip(Base):
    __tablename__ = "trip"

    id = Column(Integer, primary_key=True, index=True)
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    bikeid = Column(Integer)
    tripduration = Column(Float)
    from_station_id = Column(Integer)
    to_station_id = Column(Integer)


class Station(Base):
    __tablename__ = 'station'

    id = Column(Integer, primary_key=True)
    name = Column(String)


# Check if tables already exist before creating them
if not inspect(engine).has_table(engine, "trip") or not inspect(engine).has_table(engine, "station"):
    Base.metadata.create_all(bind=engine)
