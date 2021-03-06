from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class EtlPlugin(AirflowPlugin):
    name = "etl_plugin"
    operators = [
        operators.ZenodoDownloaderOperator
    ]
