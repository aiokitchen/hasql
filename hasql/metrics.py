from dataclasses import dataclass


@dataclass
class Metrics:
    max: int
    min: int
    idle: int
    used: int
    host: str
