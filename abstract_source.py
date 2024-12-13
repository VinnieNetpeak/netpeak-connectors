# AbstractSource class - creates a unified interface for all data sources
# LogMeta metaclass - logs all method calls
# log_method_call decorator - logs method calls and exceptions
# send_notification function - sends a notification to a Telegram chat

import logging
from functools import wraps
from datetime import datetime, timedelta
import asyncio
from telegram import Bot
from abc import abstractmethod

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Telegram bot setup
async def send_notification_async(bot, chat_id, message):
    try:
        # Escape the message
        await bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
    except Exception as e:
        logger.error(f"Failed to send notification: {str(e)}")

def send_notification(message):
    bot = Bot(token='')
    chat_id = ''
    try:
        loop = asyncio.get_running_loop()
        if loop and loop.is_running():
            # If there's an existing event loop, create a task for the coroutine
            loop.create_task(send_notification_async(bot, chat_id, message))
        else:
            # If there's no running event loop, use asyncio.run
            asyncio.run(send_notification_async(bot, chat_id, message))
    except RuntimeError:
        # If there's no running event loop, use asyncio.run
        asyncio.run(send_notification_async(bot, chat_id, message))
        
# Enable automatic logging of each method invocation
def log_method_call(class_name, method_name):
    """
    Decorator for logging method calls and exceptions.
    """
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(f"{class_name}: Starting execution of method: {method_name}")
            try:
                result = func(*args, **kwargs)
                logger.info(f"{class_name}: Successful completion of method: {method_name}")
                return result
            except Exception as e:
                logger.error(f"{class_name}: Error in method: {method_name}. Error: {str(e)}")
                raise
        return wrapper
    return decorator

class LogMeta(type):
    """
    Metaclass for logging all methods.
    """
    
    def __new__(cls, name, bases, attrs):
        for key, value in attrs.items():
            if callable(value) and not key.startswith('__'):
                attrs[key] = log_method_call(name, key)(value)
        return super(LogMeta, cls).__new__(cls, name, bases, attrs)

class AbstractSource(metaclass=LogMeta):
    """
    Abstract class for all data sources, to have universal
    logging and error handling, and to standardize the ETL process.
    """
    
    def __init__(self, config):
        self.config = config
        self.config.setdefault('date_from', datetime.strftime(datetime.now() - timedelta(days=4), '%Y-%m-%d'))
        self.config.setdefault('date_to', datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d'))
        self.config.setdefault('utc_offset_hours', 0)
        self.config.setdefault('dataset_location', 'US')

    @abstractmethod
    def validate_input(self):
        pass
    
    @abstractmethod
    def authenticate(self):
        pass

    @abstractmethod
    def fetch_all_data(self):
        pass

    @abstractmethod
    def fetch_data(self):
        pass

    @abstractmethod
    def transform_data(self):
        pass

    @abstractmethod
    def bq_schema(self):
        pass
