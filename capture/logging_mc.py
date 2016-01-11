import logging
from logging.handlers import RotatingFileHandler

def get_logger( source ):
    """
    Responsable for save logs of operations
    :return: logger configured based on source
    :rtype: logging.getLogger( source)
    
    """ 


    logger = logging.getLogger(source)
    logger.setLevel(logging.DEBUG)

# create stream handler and set level to debug
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    file_handler = RotatingFileHandler(    '/tmp/mediacloud_{0}.log'.format(source), maxBytes=5e6, backupCount=3)

# create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# add formatter to stream_handler
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

# add stream_handler to logger
    logger.addHandler(stream_handler)  # uncomment for console output of messages
    logger.addHandler(file_handler)
    
    return logger