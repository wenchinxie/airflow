import sys
import os
from airflow.plugins_manager import AirflowPlugin


def add_relative_path_to_sys(relative_path: str):
    # Define your relative path

    # Convert it to an absolute path
    absolute_path = os.path.abspath(relative_path)

    # Add the absolute path to sys.path
    sys.path.append(absolute_path)


class AirflowRouterPlugin(AirflowPlugin):
    name = "utils"
    macros = [add_relative_path_to_sys]
