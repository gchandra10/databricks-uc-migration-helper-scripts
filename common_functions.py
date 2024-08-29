import logging,os
from logging.handlers import TimedRotatingFileHandler

def logger_setup(app_name):
    """
    Sets up a logger with a TimedRotatingFileHandler and a StreamHandler.

    Returns:
        logger (logging.Logger): Configured logger.
        file_handler (TimedRotatingFileHandler): File handler for the logger.
    """

    # Create a logger with the name of the module
    logger = logging.getLogger(app_name)

    # Set the logging level of the logger to DEBUG
    # only DEBUG and higher-level messages will be processed and output by the handler
    # DEBUG = 10
    # INFO = 20
    # WARNING = 30
    # ERROR = 40
    # CRITICAL = 50
    
    logger.setLevel(logging.DEBUG)
    
    # Get the current working directory
    current_dir = os.getcwd()
    
    # Create 'logs' directory in the current working directory if it doesn't exist
    log_dir = os.path.join(current_dir, "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file_name = app_name.lower()
    # Define the log file path within the current directory
    log_file = f"{log_dir}/{log_file_name}.log"

    # This step is needed when using FileHandler as part of external PY file
    try:
        open(log_file, 'a').close()
    except OSError as e:
        print("Failed creating the file - {e}")


    # Create a TimedRotatingFileHandler that rotates the log file every 60 mins and keeps up to 7 backup files
    file_handler = TimedRotatingFileHandler(log_file, when="m", interval=60, backupCount=7)
    #file_handler = logging.FileHandler(log_file)

    # Create a StreamHandler to output logs to the console
    console_handler = logging.StreamHandler()

    # Define the log message format
    log_format = "%(asctime)s %(levelname)s [%(name)s] {%(threadName)s} %(message)s"
    
    # Create a formatter using the defined format
    log_formatter = logging.Formatter(fmt=log_format, datefmt="%H:%M:%S")

    # Set the formatter for the file handler
    file_handler.setFormatter(log_formatter)

    # Set the logging level for the file handler to DEBUG
    file_handler.setLevel(logging.DEBUG)

    # Set the formatter for the console handler
    console_handler.setFormatter(log_formatter)

    # Set the logging level for the console handler to WARNING
    console_handler.setLevel(logging.WARNING)

    # Add the file handler to the logger
    logger.addHandler(file_handler)

    # Add the console handler to the logger
    logger.addHandler(console_handler)

    return logger,file_handler

