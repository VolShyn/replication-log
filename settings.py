from typing import List

from pydantic import AnyHttpUrl, BaseSettings, Field


class Settings(BaseSettings):
    role: str = Field(default="master", env="ROLE")
    host: str = Field(default="0.0.0.0", env="HOST")
    port: int = Field(default=8000, env="PORT")
    secondaries: List[AnyHttpUrl] = Field(
        default_factory=list, env="SECONDARIES"
    )  # supports JSON

    repl_delay_secs: float = Field(default=10.0, env="REPL_DELAY_SECS")
    repl_timeout_secs: float = Field(
        default=30.0, env="REPL_TIMEOUT_SECS"
    )  # changed to 30 secs as suggested
    repl_retries: int = Field(default=2, env="REPL_RETRIES")

    # heartbeat conf
    heartbeat_interval_secs: float = Field(default=5.0, env="HEARTBEAT_INTERVAL_SECS")
    heartbeat_timeout_secs: float = Field(default=2.0, env="HEARTBEAT_TIMEOUT_SECS")
    suspect_threshold: int = Field(
        default=2, env="SUSPECT_THRESHOLD"
    )  # missed heartbeats before suspected
    unhealthy_threshold: int = Field(
        default=4, env="UNHEALTHY_THRESHOLD"
    )  # missed heartbeats before unhealthy

    # pylance fights here, just ignore
    class Config:  # type: ignore
        env_file = ".env"


settings = Settings()
