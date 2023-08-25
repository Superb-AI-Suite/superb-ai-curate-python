import datetime
import decimal
import json
from typing import Dict, List, Optional, Tuple, Union

from spb_curate import api_requestor, error, util


class SuperbAIObject(dict):
    class ReprJSONEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, datetime.datetime):
                return api_requestor._encode_datetime(obj)
            if isinstance(obj, decimal.Decimal):
                return str(obj)
            return super(SuperbAIObject.ReprJSONEncoder, self).default(obj)

    _discriminator_map = {}
    _endpoints: Dict[str, str] = {}
    _endpoints_method: Dict[str, str] = {}
    _field_class_map = {}
    _field_initializers = {}
    _object_type: str = ""
    _property_fields = set()

    def __init__(
        self,
        id: Optional[str] = None,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        **params,
    ):
        super(SuperbAIObject, self).__init__()

        object.__setattr__(self, "access_key", access_key)
        object.__setattr__(self, "team_name", team_name)

        if id:
            self["id"] = id
        else:
            self._init_volatile_fields(**params)

    def _init_volatile_fields(self, **params) -> None:
        pass

    def _init_volatile_fields_validate(
        self,
        key_value_pair_lists: List[Union[Tuple[str, any], List[Tuple[str, any]]]],
    ):
        util.validate_arguments_require_one(key_value_pair_lists)

    def _init_volatile_fields_load(self, fields: List[Tuple[str, any]]):
        for k, v in fields:
            cls = self._field_class_map.get(k, None)
            self[k] = util.convert_to_superb_ai_object(data=v, cls=cls)

    @classmethod
    def api_base(cls):
        return None

    @classmethod
    def get_endpoint(cls, *, name: str, params: Optional[dict] = None) -> Optional[str]:
        url = cls._endpoints.get(name, None)

        if url and params:
            for k, v in params.items():
                if v in ["", None]:
                    raise error.ValidationError(
                        f"The required endpoint parameter '{k}' is missing."
                    )

            url = url.format(**params)

        return url

    @classmethod
    def get_endpoint_method(cls, *, name: str, default: Optional[str] = None) -> str:
        if default is None and name not in cls._endpoints_method:
            raise error.ValidationError(
                f"The '{name}' endpoint http method is missing."
            )

        return cls._endpoints_method.get(name, default)

    def request(
        self,
        *,
        method: str,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
    ):
        return SuperbAIObject._request(
            self, method_=method, url_=url, headers=headers, params=params
        )

    def _request(
        self,
        *,
        method_: str,
        url_: str,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
    ):
        params = None if params is None else params.copy()
        team_name = team_name or self.team_name
        access_key = access_key or self.access_key

        requestor = api_requestor.APIRequestor(
            access_key=access_key,
            api_base=self.api_base(),
            team_name=team_name,
        )

        response, access_key = requestor.request(
            method=method_, url=url_, params=params, headers=headers
        )

        return util.convert_to_superb_ai_object(response, access_key, team_name, params)

    @classmethod
    def construct_from_dict(
        cls,
        *,
        data: dict,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ):
        init_params = data.copy()
        init_params.update(
            {
                "id": data.get("id", None),
                "access_key": access_key or data.get("access_key", None),
                "team_name": team_name or data.get("team_name", None),
            }
        )

        entity = cls(**init_params)
        entity.load_from_dict(data=data, access_key=access_key, team_name=team_name)
        return entity

    @classmethod
    def get_cls_by_discriminator(cls, field: str, data: dict):
        field_cls = None

        (
            discriminator_key,
            discriminator_cls_map,
        ) = cls._discriminator_map.get(field, (None, None))

        if discriminator_key and discriminator_cls_map:
            discriminator_value = data.get(discriminator_key, "")
            field_cls = discriminator_cls_map.get(discriminator_value, None)

            if field_cls is None:
                raise error.ValidationError(
                    f"'{discriminator_value}' is a not a valid discriminator for '{field}' in {cls}."
                )

        return field_cls

    def load_from_dict(
        self,
        *,
        data: dict,
        access_key: Optional[str] = None,
        team_name: Optional[str] = None,
    ):
        self.access_key = access_key or getattr(data, "access_key", None)
        self.team_name = team_name or getattr(data, "team_name", None)

        # Wipe existing values
        self.clear()

        for k, v in iter(data.items()):
            # Use the specified field initializer if available
            if self._field_initializers.get(k, None):
                getattr(self, self._field_initializers.get(k))(**data)
            else:
                cls = self._field_class_map.get(k, None)

                if cls is None:
                    # Fetch the class for field with a discriminator
                    cls = self.get_cls_by_discriminator(field=k, data=data)

                self[k] = util.convert_to_superb_ai_object(
                    data=v, access_key=access_key, team_name=team_name, cls=cls
                )

    def __str__(self):
        return json.dumps(
            self.to_dict_deep(),
            sort_keys=True,
            indent=2,
            cls=self.ReprJSONEncoder,
        )

    def to_dict(self):
        return dict(self)

    def to_dict_deep(self):
        def obj_to_dict(val):
            if val is None:
                return None
            elif isinstance(val, SuperbAIObject):
                return val.to_dict_deep()
            else:
                return val

        def process_item(val):
            return (
                list(map(obj_to_dict, val))
                if isinstance(val, list)
                else obj_to_dict(val)
            )

        self_dict = self.to_dict()

        if isinstance(self_dict, list):
            return list(map(obj_to_dict, self_dict))

        result = {}
        for k, v in iter(self_dict.items()):
            result[k] = process_item(v)

        return result

    def update(self, update_dict) -> None:
        """This method inserts the specified items to the ``SuperbAIObject``.
        Note: This method is a python ``dict`` function and does not make
        any Superb AI API calls.

        Parameters
        ----------
        update_dict
            A dictionary or iterable object with key value pairs that will
            be inserted to the SuperbAIObject.
        """
        return super(SuperbAIObject, self).update(update_dict)

    def __setattr__(self, k, v):
        if k[0] == "_" or k in self.__dict__:
            return super(SuperbAIObject, self).__setattr__(k, v)

        self[k] = v
        return None

    def __getattr__(self, k):
        if k[0] == "_":
            raise AttributeError(k)

        if k in self._property_fields:
            return self.__getattribute__(k)

        try:
            return self[k]
        except KeyError as err:
            raise AttributeError(*err.args)

    def __delattr__(self, k):
        if k[0] == "_" or k in self.__dict__:
            return super(SuperbAIObject, self).__delattr__(k)
        else:
            del self[k]

    def __setitem__(self, k, v):
        super(SuperbAIObject, self).__setitem__(k, v)

    def __getitem__(self, k):
        return super(SuperbAIObject, self).__getitem__(k)

    def __delitem__(self, k):
        super(SuperbAIObject, self).__delitem__(k)
