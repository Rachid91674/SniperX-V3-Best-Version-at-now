import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Create logs directory if it doesn't exist
LOG_DIR = 'logs'
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

def setup_logger(name, log_level=logging.INFO):
    """
    Set up a logger with both file and console handlers.
    
    Args:
        name (str): Name of the logger
        log_level: Logging level (default: logging.INFO)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # Create file handler with rotation (10MB per file, keep 5 backup files)
    log_file = os.path.join(LOG_DIR, f'sniperx_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

def log_uncaught_exceptions(exc_type, exc_value, exc_traceback):
    """Log uncaught exceptions"""
    if issubclass(exc_type, KeyboardInterrupt):
        # Call the default excepthook when keyboard interrupt is received
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    
    logger = logging.getLogger('UncaughtException')
    logger.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

# Set up default logger
logger = setup_logger('SniperX')

# Set up global exception handler
import sys
sys.excepthook = log_uncaught_exceptions

# Example usage
if __name__ == "__main__":
    logger.info("Logger initialized successfully")
    try:
        # This will generate an uncaught exception
        1 / 0
    except Exception as e:
        logger.error("This is a test error", exc_info=True)
    logger.warning("This is a warning")
    logger.info("This is an info message")
