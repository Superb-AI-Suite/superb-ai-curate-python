import logging
import os
import re
import sys
from typing import Dict, List, Optional, Tuple, Union

import spb_curate
from spb_curate import error
from spb_curate.abstract.superb_ai_object import SuperbAIObject
from spb_curate.superb_ai_response import SuperbAIResponse

SPB_LOG_LEVEL = os.environ.get("SPB_LOG_LEVEL")
LOG_LEVELS = ["DEBUG", "INFO"]
logger = logging.getLogger("superb-ai")


def _console_log_level() -> Optional[str]:
    if str(spb_curate.log_level).upper() in LOG_LEVELS:
        return str(spb_curate.log_level).upper()
    elif str(SPB_LOG_LEVEL).upper() in LOG_LEVELS:
        return str(SPB_LOG_LEVEL).upper()
    else:
        return None


def log_debug(message, **params):
    msg = logfmt(dict(message=message, **params))
    if _console_log_level() == "DEBUG":
        print(msg, file=sys.stderr)
    logger.debug(msg)


def log_info(message, **params):
    msg = logfmt(dict(message=message, **params))
    if _console_log_level() in LOG_LEVELS:
        print(msg, file=sys.stderr)
    logger.info(msg)


def logfmt(props):
    def fmt(key, val):
        if hasattr(val, "decode"):
            val = val.decode("utf-8")
        if not isinstance(val, str):
            val = str(val)
        if re.search(r"\s", val):
            val = repr(val)
        if re.search(r"\s", key):
            key = repr(key)
        return "{key}={val}".format(key=key, val=val)

    return " ".join([fmt(key, val) for key, val in sorted(props.items())])


def get_object_mapping() -> Dict[str, SuperbAIObject]:
    """Get the's the object class mapped against the 'object type'.
    Using this function avoids a circular import.

    Returns
    -------
        The dictionary of the object type mappings.
    """
    from spb_curate.object_mapping import OBJECT_MAPPING

    return OBJECT_MAPPING


def convert_to_superb_ai_object(
    *,
    data: Union[SuperbAIObject, SuperbAIResponse, list, dict],
    access_key: Optional[str] = None,
    team_name: Optional[str] = None,
    cls=None,
):
    if isinstance(data, SuperbAIResponse):
        data = data.data

    if isinstance(data, list):
        return [
            convert_to_superb_ai_object(
                data=x, access_key=access_key, team_name=team_name, cls=cls
            )
            for x in data
        ]
    elif isinstance(data, dict) and not isinstance(data, SuperbAIObject):
        input_data = data.copy()

        if cls is None:
            cls = SuperbAIObject

        # Get the object type mapping, if it doesn't exist then default to
        # using the cls.
        cls = get_object_mapping().get(data.get("_object_type", ""), cls)

        return cls.construct_from_dict(
            data=input_data, access_key=access_key, team_name=team_name
        )
    else:
        return data


def validate_argument_value(key: str, value: any, is_required: bool) -> any:
    if is_required and value is None:
        raise error.ValidationError(f"{key} is required")
    return value


def validate_argument_list(
    keys: List[str], values: List[any], is_required: bool
) -> any:
    # TODO: Use `validate_arguments_require_one` instead.
    valid_count = 0
    for i in range(len(keys)):
        try:
            validate_argument_value(
                key=keys[i], value=values[i], is_required=is_required
            )
            valid_count += 1
        except error.ValidationError:
            pass

    if is_required and valid_count == 0:
        raise error.ValidationError(f"At least one of fields {keys} is required")

    return values


def validate_arguments_require_all(items: List[Tuple[str, any]]):
    """Validates that all of the provided key-value fields are valid.

    Parameters
    ----------
    items
        Key-value tuples that represent required fields.
    """
    for key, value in iter(items):
        validate_argument_value(key=key, value=value, is_required=True)


def validate_arguments_require_one(
    items: List[Union[Tuple[str, any], List[Tuple[str, any]]]]
):
    """Validates that one of the provided fields are valid.
    If the list item is a list itself, then it will require all the
    items within to have valid values.

    Parameters
    ----------
    items
        Key-value tuples or list of the tuples that represent required fields.

    Raises
    ------
    error.ValidationError
        Thrown when a required field is missing.
    """
    valid_items = 0
    n = len(items)

    for i in range(n):
        item = items[i]
        try:
            # if item is a list process that list of arguments as all required
            if isinstance(item, list):
                validate_arguments_require_all(items=item)
            else:
                key, value = item
                validate_argument_value(key=key, value=value, is_required=True)
        except error.ValidationError:
            pass
        else:
            valid_items += 1

    if valid_items == 0:
        error_message = ""
        for i in range(n):
            if isinstance(items[i], list):
                error_message += ", ".join(map(lambda x: x[0], items[i]))
            else:
                error_message += f"{items[i][0]}"

            if i < n - 1:
                error_message += " or "

        raise error.ValidationError(
            f"At least one of fields {error_message} is required"
        )
