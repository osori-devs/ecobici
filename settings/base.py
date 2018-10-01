# -*- coding: utf-8 -*-

import os


# Settings file.


class ImproperlyConfiguredException(Exception):
    """ Custom exception class when the variable isnt define """
    pass


def get_env_var(var_name, default=None):
    """ function to read environment variables in settings """
    try:
        return os.environ[var_name]
    except KeyError:
        if default is None:
            error_msg = "Set the {} environment variable".format(var_name)
            raise ImproperlyConfiguredException(error_msg)
        return default


ECOBICI_API_CLIENT_SECRET = get_env_var("CLIENT_SECRET")

ECOBICI_API_CLIENT_ID = get_env_var("CLIENT_ID")

# CSV data files directory
DATA_DIR = "data/csv"

# Amount of rows to commit to the database when reading from csv files
ROWS_TO_WRITE = 2000
