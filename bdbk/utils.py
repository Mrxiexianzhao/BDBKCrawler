# -*- coding: utf-8 -*-

import os
import errno
import datetime


def mkdir(path):
    try:
        os.makedirs(path)
    except OSError, err:
        if err.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def now_string():
    return datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S %f")
    
