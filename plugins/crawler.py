import sys

sys.extend(["/root/crawler"])

import crawler


class AirflowRouterPlugin(AirflowPlugin):
    name = "crawler"
    macros = [crawler]
