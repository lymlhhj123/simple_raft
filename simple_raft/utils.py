# coding: utf-8

import os


def ensure_dir(dir_name):
    """create dir"""
    os.makedirs(dir_name, exist_ok=True)
