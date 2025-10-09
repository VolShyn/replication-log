from pydantic import BaseSettings, AnyHttpUrl, Field
from typing import List

class Settings(BaseSettings):
    role: str = Field(default="master", env="ROLE")
    host: str = Field(default="0.0.0.0", env="HOST")
    port: int = Field(default=8000, env="PORT")
    secondaries: List[AnyHttpUrl] = Field(default_factory=list, env="SECONDARIES")  # supports JSON
    repl_delay_secs: float = Field(default=0.0, env="REPL_DELAY_SECS")
    repl_timeout_secs: float = Field(default=3.0, env="REPL_TIMEOUT_SECS")
    repl_retries: int = Field(default=2, env="REPL_RETRIES")

    # pylance fights here, just ignore
    class Config: # type: ignore
        env_file = ".env"  

settings = Settings()
