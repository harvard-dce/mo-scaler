
class OpsworksControllerException(Exception):
    """Generic controller exception"""


class OpsworksScalingException(OpsworksControllerException):
    """Issues with scaling up/down/auto"""


class MatterhornCommunicationException(Exception):
    """MH API failures"""


class MatterhornNodeException(Exception):
    """MH node mapping problems"""
