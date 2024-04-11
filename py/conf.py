import os
import logging

# Configure logging to a file
logging.basicConfig(
    filename='app.log',  # Specify the filename
    level=logging.INFO,  # Set the logging level (e.g., DEBUG, INFO, WARNING, ERROR)
    format='%(asctime)s - %(levelname)s - %(message)s'  # Define the log message format
)

# play speed, defaults to 1
PLAY_SPEED = 0.1
assert PLAY_SPEED >=0.01 and PLAY_SPEED <= 1



