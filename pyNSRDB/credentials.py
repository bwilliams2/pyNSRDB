from typing import Dict, Optional
from pathlib import Path
from dotenv.main import dotenv_values


def _get_user_credentials(
    api_key: Optional[str] = None,
    full_name: Optional[str] = None,
    affiliation: Optional[str] = None,
    email: Optional[str] = None,
    reason: Optional[str] = None,
    mailing_list: Optional[bool] = None,
) -> Dict[str, str]:
    """Reads user credential file in user's home directory.

    Args:
        api_key (Optional[str], optional): [description]. Defaults to None.
        full_name (Optional[str], optional): [description]. Defaults to None.
        affiliation (Optional[str], optional): [description]. Defaults to None.
        email (Optional[str], optional): [description]. Defaults to None.
        reason (Optional[str], optional): [description]. Defaults to None.
        mailing_list (Optional[bool], optional): [description]. Defaults to
            None.

    Raises:
        FileNotFoundError: Raised if file is not found.
        ValueError: Raised if credential file does not contain entry for
            API_KEY.

    Returns:
        Dict[str, str]: Dictionary of credential values.
    """
    home = Path.home()
    config_file = home.joinpath(".pyNSRDB")
    # config_file doesn't matter if api_key is provided
    if config_file.exists():
        user_config = {
            k.lower(): v for k, v in dotenv_values(config_file).items()
        }
        if "api_key" not in user_config:
            raise ValueError(
                "NSRDB credentials do not contain an `API_KEY`"
                "value. Please see documentation for setup instructions."
            )
    elif api_key is None:
        raise FileNotFoundError(
            "NSRDB credentials not found. Please see documentation for"
            "setup information."
        )
    else:
        config_file = {}

    # Provided items take precedent over config file?
    items = zip(
        [
            "api_key",
            "full_name",
            "email",
            "affiliation",
            "reason",
            "mailing_list",
        ],
        [api_key, full_name, email, affiliation, reason, mailing_list],
    )
    user_config.update({k: v for k, v in items if v is not None})
    return user_config
