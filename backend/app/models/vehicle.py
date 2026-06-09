from typing import Optional
from pydantic import BaseModel


class VehicleUpdate(BaseModel):
    VehicleNumber: int
    Latitude: float
    Longitude: float
    Speed: float
    Heading: float
    RouteNumber: str
    DoorStatus: int
    OccupancyLevel: int
    Acceleration: float
    ScheduleOffset: int
    Timestamp: str
    LocationSource: Optional[str] = "GPS"
    LineID: Optional[int] = None
    OperatorID: Optional[int] = None
    VehicleType: Optional[str] = None


class StatsResponse(BaseModel):
    active_vehicles: int
    avg_speed: float
    messages_per_second: int
    doors_open: int
