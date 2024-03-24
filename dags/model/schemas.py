from pydantic import BaseModel, model_validator
from datetime import datetime


class Trip(BaseModel):
    id: int
    start_time: datetime
    end_time: datetime
    bikeid: int
    tripduration: float
    from_station_id: int
    to_station_id: int

    @model_validator(mode='before')
    def validate_float(cls, data):
        if trip_duration := data.get('tripduration'):
            data['tripduration'] = trip_duration.replace(',', '')

        return data

    class Config:
        orm_mode = True


