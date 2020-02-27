"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        if "com.udacity.weather.v1" == message.topic():
            logger.info("weather process_message")
            json_data = json.loads(message.value())
            self.temperature = json_data.get("temperature")
            self.status = json_data.get("status")
        else:
            logger.info("it is not an weather message")
