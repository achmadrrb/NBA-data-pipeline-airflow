# This file marks the directory as a Python package.
# It can be empty or contain package initialization code.
# The actual configuration is defined in config/config.py.
from .config import CONFIG, Config  # Import the Config class from config/config.py

__all__ = ["CONFIG", "Config"]
# This allows for easy access to the Config class and its attributes.
# You can add more configurations or utility functions here as needed.
