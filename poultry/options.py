import codecs
import inspect
import logging
import sys

import opster

from poultry.config import Config
from poultry.producers import from_stream

logger = logging.getLogger(__name__)


class Dispatcher(opster.Dispatcher):
    def __init__(self, *globaloptions):
        globaloptions = (
            tuple(globaloptions) +
            (
                ('v', 'verbose', False, 'Be verbose.'),
                ('c', 'config', Config.default_config_file, 'Configuration file'),
                ('s', 'source', '', 'The tweet source.'),
            )
        )

        super(Dispatcher, self).__init__(
            globaloptions=globaloptions,
            middleware=_middleware,
        )


def _middleware(func):
    def wrapper(*args, **kwargs):
        # Always force utf8 output.
        sys.stdout = codecs.getwriter('utf8')(sys.stdout)

        if func.__name__ == 'help_inner':
            return func(*args, **kwargs)

        f_args = inspect.getargspec(func)[0]

        verbose = kwargs.pop('verbose')

        config = kwargs['config'] = Config(kwargs.get('config'))
        if 'config' not in f_args:
            kwargs.pop('config')

        source = kwargs.pop('source')
        producer = lambda target: from_stream(target, source, config)

        if 'producer' in f_args:
            kwargs['producer'] = producer

        logger = logging.getLogger('poultry')

        if verbose:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)-6s: %(name)s - %(levelname)s - %(message)s')

            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
        else:
            logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.CRITICAL)

        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            pass
        except Exception:
            logger.error('Middleware captured an exception', exc_info=True)
            raise

    return wrapper
