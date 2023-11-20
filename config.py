import os
import logging
import yaml


class Config:
    """
    Config class to create read and update the yaml config file
    """

    # Class variable
    _yaml_config = None  # type: dict
    _yaml_file = None  # type: str
    _kwargs = None  # type: dict

    logger = logging.getLogger()
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    def __init__(self, **kwargs) -> None:
        self.logger.info("Initialize the class Config")
        self._kwargs = kwargs

    def config(self) -> None:
        """"Method overwrite the execution of the process
        Return:
            None
        """
        self._read_yaml_file()

    def _read_yaml_file(self) -> None:
        """
        Read the Yaml file configuration and set into the class variable

        :return: None
        """
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self._yaml_file = os.path.join(current_dir, "config.yml")
        with open(self._yaml_file) as f:
            data = yaml.safe_load(f)

        if data:
            self._yaml_config = data
