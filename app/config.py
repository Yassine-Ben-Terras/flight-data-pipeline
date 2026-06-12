"""Configuration shared by local commands and containers; never log secrets."""
import math
import os
from dataclasses import dataclass


def credit_cost(area: float) -> int:
    return 1 if area <= 25 else 2 if area <= 100 else 3 if area <= 400 else 4


@dataclass(frozen=True)
class Settings:
    bootstrap: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic: str = os.getenv("KAFKA_TOPIC", "flight-states")
    group: str = os.getenv("KAFKA_GROUP_ID", "flight-warehouse-v1")
    mode: str = os.getenv("SOURCE_MODE", "demo")
    credentials_file: str = os.getenv("OPENSKY_CREDENTIALS_FILE", "/run/secrets/opensky.json")
    coverage: str = os.getenv("OPENSKY_COVERAGE", "regional")
    lamin: float = float(os.getenv("BBOX_LAMIN", "33"))
    lomin: float = float(os.getenv("BBOX_LOMIN", "-7"))
    lamax: float = float(os.getenv("BBOX_LAMAX", "37"))
    lomax: float = float(os.getenv("BBOX_LOMAX", "-2"))
    budget: int = int(os.getenv("DAILY_CREDIT_BUDGET", "3600"))
    retention: int = int(os.getenv("RAW_RETENTION_DAYS", "14"))

    def __post_init__(self):
        if self.coverage not in {"global", "regional"}:
            raise ValueError("OPENSKY_COVERAGE must be global or regional")
        if not (-90 <= self.lamin < self.lamax <= 90 and -180 <= self.lomin < self.lomax <= 180):
            raise ValueError("Invalid bounding box")
        if self.budget <= 0 or not 1 <= self.retention <= 365:
            raise ValueError("Budget must be positive; retention must be 1–365 days")
        if self.mode not in {"demo", "live"}:
            raise ValueError("SOURCE_MODE must be demo or live")

    @property
    def bbox(self):
        if self.coverage == "global":
            return {}
        return {"lamin": self.lamin, "lomin": self.lomin, "lamax": self.lamax, "lomax": self.lomax}

    @property
    def cost(self):
        if self.coverage == "global":
            return 4
        return credit_cost((self.lamax - self.lamin) * (self.lomax - self.lomin))

    @property
    def interval(self):
        return max(5, math.ceil(86400 * self.cost / self.budget))


def connect():
    import psycopg
    return psycopg.connect(host=os.getenv("PGHOST", "postgres"),
                          dbname=os.getenv("PGDATABASE", "flight_warehouse"),
                          user=os.getenv("PGUSER", "pipeline"),
                          password=os.environ["POSTGRES_PASSWORD"],
                          port=os.getenv("PGPORT", "5432"), connect_timeout=15)
