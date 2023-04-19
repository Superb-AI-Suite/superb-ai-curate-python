import configparser
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ENV_ACCESS_KEY = "SPB_ACCESS_KEY"
ENV_PROFILE = "SPB_PROFILE"
ENV_TEAM_NAME = "SPB_TEAM_NAME"


class Config:
    access_key = None
    profile = None
    team_name = None

    def __init__(self, profile: Optional[str] = None) -> None:
        self.profile = profile or os.getenv(ENV_PROFILE, "default")
        self._set_credentials(profile=profile or os.getenv(ENV_PROFILE, "default"))

    def _read_config(self, *, credential_path: Path, profile: str) -> Dict[str, str]:
        config = configparser.ConfigParser()
        config.read(filenames=credential_path)
        config_key_map = {"account_name": "team_name"}

        def parse_creds(*, keys: List[str]) -> Optional[Dict[str, str]]:
            credential = {}

            for k in keys:
                try:
                    credential[config_key_map.get(k, k)] = config.get(profile, k)
                except (configparser.NoSectionError, configparser.NoOptionError):
                    return None

            return credential

        credential = parse_creds(keys=["team_name", "access_key"])

        if credential is None:
            credential = parse_creds(keys=["access_key", "account_name"])

        return credential

    def _set_credentials(self, *, profile: str) -> None:
        self.access_key = os.getenv(ENV_ACCESS_KEY)
        self.team_name = os.getenv(ENV_TEAM_NAME)

        if not self.access_key or not self.team_name:
            config_path = Path.home().joinpath(".spb", "config")

            if config_path.exists():
                credential = self._read_config(
                    credential_path=config_path, profile=self.profile
                )

                self.access_key = self.access_key or credential.get("access_key", None)
                self.team_name = self.team_name or credential.get("team_name", None)
