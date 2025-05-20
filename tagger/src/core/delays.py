import random
import time
from logs.logger import setup_logger

logger = setup_logger()

def delay_request(min_delay=3, max_delay=6):
    delay = random.uniform(min_delay, max_delay)
    time.sleep(delay)

