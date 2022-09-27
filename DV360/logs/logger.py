import logging

def logger(log_level):
    '''
    creates custom test logger

    :param log_level: desired logging level
    returns
        logger: customized logger
    '''
    # gets name of class/method from where this method is called
    logger = logging.getLogger(__name__)

    # by default, log all messages
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)

    formatter = logging.Formatter('%(asctime)s: - %(levelname)s - %(filename)s: %(funcName)s: %(message)s',
                                  datefmt='%m/%d/%Y %I:%M:%S %p')
    console_handler.setFormatter(formatter)

    if not logger.hasHandlers():
        logger.addHandler(console_handler)

    return logger