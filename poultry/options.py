import logging
import inspect

import opster

from poultry.config import Config


logger = logging.getLogger(__name__)


class Dispatcher(opster.Dispatcher):
    def __init__(self, *globaloptions):
        globaloptions = (
            tuple(globaloptions) +
            (
                ('v', 'verbose', False, 'Be verbose.'),
                ('c', 'config', Config.default_config_file, 'Configuration file'),
            )
        )

        super(Dispatcher, self).__init__(
            globaloptions=globaloptions,
            middleware=_middleware,
        )


def _middleware(func):
    def wrapper(*args, **kwargs):
        if func.__name__ == 'help_inner':
            return func(*args, **kwargs)

        f_args = inspect.getargspec(func)[0]

        verbose = kwargs.pop('verbose')

        logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
        logger = logging.getLogger('poultry')

        if verbose:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)-6s: %(name)s - %(levelname)s - %(message)s')

            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)

        kwargs['config'] = Config(kwargs.get('config'))
        if 'config' not in f_args:
            kwargs.pop('config')

        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.error('Middleware captured an exception', exc_info=True)
            raise e

    return wrapper
