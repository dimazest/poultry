import os
import codecs
try:
    from ConfigParser import SafeConfigParser, NoOptionError, NoSectionError
except ImportError:
    from configparser import SafeConfigParser, NoOptionError, NoSectionError
from sys import version_info
PY2 = version_info < (3, )

from poultry.tweet import Coordinates, Tweet


class Config(object):

    default_config_file = os.path.join(os.path.expanduser('~'), '.poultry.cfg')

    def __init__(self, config_file):
        config = SafeConfigParser()
        config.read_file(codecs.open(config_file, 'r', 'utf-8'))
        self.config = config

    def sections(self):
        return self.config.sections()

    def items(self, sections):
        return self.config.items(sections)

    def get_elements(self, section, option):
        value = self.config.get(section, option)
        if PY2:
            value = value.decode('utf-8')
        return tuple(filter(None, (value.strip().split('\n'))))

    @property
    def dustbin_template(self):
        try:
            template = self.config.get('poultry', 'dustbin_template', raw=True)
        except NoSectionError:
            pass
        except NoOptionError:
            pass
        else:
            return template if template.strip() else None

    @property
    def _filter_sections(self):
        return (s for s in self.config.sections() if s.startswith('filter:'))

    @property
    def filters(self):
        #XXX would be nice to return a pair (s, self.filter(s))
        return (self.filter(s) for s in self._filter_sections)

    def filter(self, filter_name):
        # Gather follow, track and language predicates
        predicates = {p: self.get_elements(filter_name, p) for p in 'follow track language'.split()}
        # Add locations
        predicates['locations'] = tuple(
            Coordinates.from_string(l) for l in self.get_elements(filter_name, 'locations')
        )
        split_template = self.config.get(filter_name, 'split_template', raw=True)
        try:
            start_date = self.config.get(filter_name, 'start_date')
        except NoOptionError:
            start_date = None

        try:
            end_date = self.config.get(filter_name, 'end_date')
        except NoOptionError:
            end_date = None

        return Filter(name=filter_name, split_template=split_template,
                      start_date=start_date, end_date=end_date,
                      **predicates)

    @property
    def global_filter(self):
        """The union of all the defined filters."""
        filters = iter(self.filters)
        result = next(filters)

        for filter_ in filters:
            result = result | filter_

        return result

    def merge(self, merge_name):
        inputs = self.get_elements(merge_name, 'inputs')
        return Merge(name=merge_name, inputs=inputs)

    @property
    def sentry_dsn(self):
        return self.config.get('fowler', 'sentry_dsn')


class Filter(object):
    """Twitter filter predicates.

    The predicates are::

      * https://dev.twitter.com/docs/streaming-api/methods#follow
      * https://dev.twitter.com/docs/streaming-api/methods#track
      * https://dev.twitter.com/docs/streaming-api/methods#locations
      * https://dev.twitter.com/streaming/overview/request-parameters#language

    """
    def __init__(self, follow=None, track=None, locations=None, language=None,
                 split_template=None, start_date=None, end_date=None,
                 name=None):
        self.follow = set(follow) if follow is not None else set()
        self.track = set(track) if track is not None else set()
        self.locations = set(locations) if locations is not None else set()
        self.language = set(language) if language is not None else set()

        self.split_template = split_template
        # XXX that's kind of a hack
        # self.name = name.replace(':', '_') if name is not None else None
        self.name = name

        self.start_date = Tweet._created_at_to_datetime(start_date) if start_date is not None else None
        self.end_date = Tweet._created_at_to_datetime(end_date) if end_date is not None else None

    def __or__(self, other):
        filtering_predicates = 'follow', 'track', 'locations', 'language'

        predicates = {p: self.predicates[p] for p in filtering_predicates}
        for p in filtering_predicates:
            predicates[p].update(other.predicates[p])

        return Filter(**predicates)

    @property
    def predicates(self):
        return {'follow': self.follow,
                'track': self.track,
                'locations': self.locations,
                'start_date': self.start_date,
                'end_date': self.end_date,
                'language': self.language,
                }


class Merge(object):

    def __init__(self, name, inputs):
        self.name = name
        self.inputs = inputs
