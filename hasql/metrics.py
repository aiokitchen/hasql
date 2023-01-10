from dataclasses import dataclass


@dataclass(frozen=True)
class Metrics:
    max: int
    min: int
    idle: int
    used: int
    host: str
