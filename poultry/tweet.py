import json
import logging
import unicodedata
import sys

from collections import namedtuple
from datetime import datetime
from email.utils import parsedate_tz
from itertools import chain


logger = logging.getLogger(__name__)
TWEET_CREATED_AT_CHANGE_WARNING = False


class Tweet(object):
    def __init__(self, raw_json):

        if isinstance(raw_json, dict):
            self.parsed = raw_json
            self.raw = None
        else:
            try:
                tweet = json.loads(raw_json)
            except ValueError:
                raise TweetValueError("The passed json can't be parsed.")
            else:
                if isinstance(tweet, dict) and ('text' in tweet or 'full_text' in tweet):
                    self.raw = raw_json
                    self.parsed = tweet
                else:
                    raise TweetValueError("There is no 'text' field in the passed json.")

    @property
    def text(self):
        """The unprocessed text of the tweet."""
        return self.parsed['full_text'] if 'full_text' in self.parsed else self.parsed['text']

    @property
    def hashtags(self):
        '''
        The lowercased hashtags that are in the tweet.
        '''
        return set(h['text'].lower() for h in self.parsed['entities']['hashtags'])

    @property
    def urls(self):
        '''
        The urls that are in the tweet.
        '''
        return set(u['url'] for u in self.parsed['entities']['urls'])

    @property
    def user_mentions(self):
        '''
        The mentioned users in the tweet.
        '''
        return set(m['screen_name'] for m in self.parsed['entities']['user_mentions'])

    @property
    def user_mention_ids(self):
        '''
        The IDs of the mentioned users in the tweet.
        '''
        return set(m['id'] for m in self.parsed['entities']['user_mentions'])

    @property
    def screen_name(self):
        try:
            return self.parsed['user']['screen_name']
        except KeyError:
            logger.warning("An error accessing ['user']['screen_name']")

    @property
    def user_id(self):
        return self.parsed['user']['id']

    @property
    def created_at(self):
        '''
        Tweet's creation time as a datetime object.
        '''
        # XXX a hack to make it possible to change the creation time
        # of a tweet.
        _created_at = getattr(self, '_created_at', None)
        if _created_at:
            return _created_at

        return self.orig_created_at

    @created_at.setter
    def created_at(self, value):
        global TWEET_CREATED_AT_CHANGE_WARNING

        if not TWEET_CREATED_AT_CHANGE_WARNING:
            logger.warn('{} created_at property is changed. '
                        'This change is not reflected in '
                        'self.raw!'
                        ''.format(type(self)))
            TWEET_CREATED_AT_CHANGE_WARNING = True

        self._created_at = value

    @property
    def orig_created_at(self):
        try:
            created_at = self.parsed['created_at']
        except KeyError:
            logger.warning("An error accessing ['created_at']")
        else:
            return self._created_at_to_datetime(created_at)

    @staticmethod
    def _created_at_to_datetime(created_at):
        '''
        Convert a date represented in the Twitter format to a datetime
        object.
        '''
        time_tuple = parsedate_tz(created_at)
        return datetime(*time_tuple[:6])

    @property
    def retweeted_status(self):
        try:
            return Tweet(json.dumps(self.parsed['retweeted_status']))
        except ValueError:
            pass
        except KeyError:
            pass

    @property
    def id(self):
        return self.parsed['id']

    @property
    def lang(self):
        return self.parsed['lang']

    @property
    def bounding_box(self):
        """The bounding box of the tweet.

        `None` is returned in case there is no geo information.

        """
        try:
            coor = self.parsed['coordinates']
        except KeyError:
            pass
        else:
            if coor and coor['type'] == 'Point':
                c = Coordinates(*coor['coordinates'])
                return [
                    [c] * 4
                ]

        try:
            place = self.parsed['place']
        except KeyError:
            pass
        else:
            if place and place['bounding_box']:
                result = [
                    [Coordinates(*p) for p in place['bounding_box']['coordinates'][0]]
                ]
                return result

    @property
    def coordinates(self):
        """The coordinates of the tweet."""
        return (self.bounding_box or [[None]])[0][0]

    @property
    def twitter_url(self):
        '''
        Tweet's url at twitter.com
        '''
        return 'https://twitter.com/#!/{t.screen_name}/status/{t.id}'.format(t=self)

    @property
    def text_without_entities(self):
        '''
        The text of the tweet without entities (hashtags, ursl and
        user mentions).
        '''
        entities = self.parsed.get('entities', {}).values()
        indicies = list(chain.from_iterable((e['indices'] for e in es) for es in entities))

        text = list(self.text)
        for start, end in indicies:
            length = end - start
            text[start:end] = [None] * length

        return u''.join(filter(None, text))

    @property
    def tokens(self):
        '''
        Tokenized text of the tweet.

        The minimal length of a token is 3. Only letters and numbers
        make up tokens.

        Hashtags, urls, and user mentions are not included.
        '''
        return self.get_tokens()

    def get_tokens(self, min_token_len=3, allowed_categories='LN'):
        """Tokenized text of the tweet.

        :param min_token_len: The minimal length of a token. Strings
                              less than this value are not considered
                              to be tokens.

        :param allowed_categories: The categories of which a string
                                   should contain in order to be
                                   considered as a token. Refer to the
                                   `unicodedata` module documentation.

        Hashtags, urls, and user mentions are not included.

        http://www.fileformat.info/info/unicode/category/index.htm

        """
        tokens = self.text_without_entities.split()
        tokens = (t.lower() for t in tokens)

        return list(
            filter(
                lambda s: len(s) >= min_token_len,
                [
                    u''.join(c for c in t if unicodedata.category(c)[0] in allowed_categories)
                    for t in tokens
                ]
            )
        )

    @property
    def is_spam(self):
        '''
        Indication whether the tweet is considered as spam.

        .. todo: parameters instead of magic numbers.
        '''
        entity_violations = (len(e) > 2 for e in [self.hashtags, self.urls, self.user_mentions])
        lenght_violantion = len(self.tokens) < 5

        return any(entity_violations) or lenght_violantion

    def filter(self,
               follow=None,
               track=None,
               locations=None,
               language=None,
               start_date=None,
               end_date=None,
               ):
        '''
        Match the predicates to the tweet.

        :return: `True` if there is a match, `False` otherwise.

        Mimics Twitter `statuses/filter`_ method of the Streaming API.

        .. _statuses/filter: https://dev.twitter.com/docs/streaming-api/methods#statuses-filter
        '''

        if start_date is not None and self.created_at < start_date:
            return False

        if end_date is not None and end_date < self.created_at:
            return False

        follow = set(int(f) for f in follow) if follow is not None else set()

        try:
            unicode
        except NameError:
            unicode = str

        track = set(unicode(t.lower()) for t in track) if track is not None else set()
        locations = set(locations) if locations is not None else set()
        language = set(language) if language is not None else set()

        common_follow = follow.intersection(self.user_mentions) or self.user_id in follow
        common_track = (t.lower() in self.text.lower() for t in track)
        common_language = self.lang in language if language else True

        coor = self.coordinates
        place = self.parsed['place']
        if coor:
            common_location = (
                sw[0] <= coor.lon <= ne[0] and
                sw[1] <= coor.lat <= ne[1]
                for sw, ne in locations
            )
        elif place and place['bounding_box']:
            # XXX Is it always a rectangle?
            bounding_box = place['bounding_box']['coordinates'][0]

            # XXX not the best way to intersect the given location and
            # the tweet's place polygon.
            sw_lon = min(lon for lon, lat in bounding_box)
            sw_lat = min(lat for lon, lat in bounding_box)
            ne_lon = max(lon for lon, lat in bounding_box)
            ne_lat = max(lat for lon, lat in bounding_box)

            common_location = (not (sw[0] > ne_lon or
                                    ne[0] < sw_lon or
                                    ne[1] > sw_lat or
                                    sw[1] < ne_lat)
                               for sw, ne in locations)

        else:
            common_location = []

        search_terms = ([follow, track, locations])
        common = any(
            [
                common_follow,
                any(common_track),
                any(common_location),
            ]
        )

        return bool(not any(search_terms) or common) and common_language

    def __unicode__(self):
        return (
            u'{t.screen_name}: {t.text}\n'
            u'{t.twitter_url}\n'
            u'{geo}'
            u'{t.created_at}'
            ''.format(
                t=self,
                geo='http://www.openstreetmap.org/?mlat={c.lat}&mlon={c.lon}&zoom=6\n'.format(c=self.coordinates) if self.coordinates else '',
            )
        )

    def __repr__(self):
        return ('<{s.__class__.__name__}('
                'created_at={s.created_at}'
                ')>'.format(s=self)
                )

    if sys.version_info[0] >= 3: # Python 3
        def __str__(self):
            return self.__unicode__()
    else:  # Python 2
        def __str__(self):
            return self.__unicode__().encode('utf8')


class TweetValueError(ValueError):
    '''Thrown when a class:`Tweet` can't be built.'''


class Coordinates(namedtuple('Coordiantes', 'lon lat')):
    @staticmethod
    def from_string(s):
        def two_pairs(x):
            x = tuple(x)
            return x[:2], x[2:]

        return two_pairs(float(i.strip()) for i in s.split(','))


def intersect(p1, p2):
    def box(p):
        sw = Coordinates(min(c.lon for c in p), min(c.lat for c in p))
        ne = Coordinates(max(c.lon for c in p), max(c.lat for c in p))

        return sw, ne

    a_sw, a_ne = box(p1)
    b_sw, b_ne = box(p2)

    checks = [
        (a_sw.lat > b_ne.lat),
        (a_sw.lon > b_ne.lon),
        (a_ne.lat < b_sw.lat),
        (a_ne.lon < b_sw.lon),
    ]

    return not any(checks)
