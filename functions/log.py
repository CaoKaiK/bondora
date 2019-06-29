# -*- coding: utf-8 -*-
import logging
import datetime as dt
import os

def custom_logger(name):
    logger = logging.getLogger(name)
    if not len(logger.handlers):
        logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s/%(funcName)s - %(message)s')

        # define directory
        dirName = os.path.join('logs')

        # check if directories exist if not create it
        if not os.path.exists(dirName):
            os.makedirs(dirName)

        now = dt.datetime.now()
        now_string = now.strftime('%Y-%m-%d_%H_%M_%S')
        filename = f'{now_string}.log'
        filepath = os.path.join(dirName, filename)
        file_handler = logging.FileHandler(filepath)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(logging.INFO)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        
        now_string = now.strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f'Logging started on: {now_string}')
        logger.info('################################')
    return logger


