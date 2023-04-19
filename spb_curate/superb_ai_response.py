import json
from collections import OrderedDict


class SuperbAIResponse(object):
    def __init__(self, body, code, headers):
        self.code = code
        self.headers = headers
        self.body = body
        self.data = json.loads(body, object_pairs_hook=OrderedDict) if body else None

    @property
    def idempotency_key(self):
        try:
            return self.headers["idempotency-key"]
        except KeyError:
            return None

    @property
    def request_id(self):
        try:
            return self.headers["request-id"]
        except KeyError:
            return None
