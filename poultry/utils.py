import os
import platform
from itertools import chain


if platform.python_implementation == 'CPython':
    import ujson as json
else:
    import json


def get_file_names(input_dir):
    if input_dir:
        file_names = sorted(chain.from_iterable((os.path.join(p, f) for f in fs) for p, _, fs in os.walk(input_dir)))
    else:
        file_names = []

    for f in file_names:
        yield f
