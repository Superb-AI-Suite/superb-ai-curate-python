from spb_curate.config import Config

api_base = "https://api.superb-ai.com"
access_key = None
config = Config()
team_name = None
default_http_client = None
bulk_upload_bytes_max = 256000000  # 256MB
bulk_upload_object_max = 100
timeout = 300  # seconds
log_level = None  # Log level options: DEBUG, INFO

from spb_curate import error
from spb_curate.curate import *  # noqa
from spb_curate.version import VERSION
