# -*- coding: utf-8 -*-

import os
import errno


def mkdir(path):
    try:
        os.makedirs(path)
    except OSError, err:
        if err.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

    
