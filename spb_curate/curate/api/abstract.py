from spb_curate import util
from spb_curate.abstract import SuperbAIObject


class BaseImageSource(SuperbAIObject):
    def __init__(
        self,
        *,
        type: str,
        **params,
    ):
        super(BaseImageSource, self).__init__(type=type, **params)

        for k, v in iter([("type", type)]):
            self[k] = util.convert_to_superb_ai_object(data=v)
