import logging
import sys

# configure logging
logger = logging.getLogger('pipeline')
logger.setLevel(logging.DEBUG)

# log to file
file_handler = logging.FileHandler('/var/log/pipeline.log')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# log to stderr
stream_handler = logging.StreamHandler(sys.stderr)
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
