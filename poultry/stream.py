import logging
from time import sleep
from itertools import chain
from threading import Thread
try:
    from Queue import Queue, Full
except ImportError:
    from queue import Queue, Full

try:
    from urllib.parse import quote
except ImportError:
    from urllib import quote

from sys import version_info
PY2 = version_info < (3, )


import requests
from requests_oauthlib import OAuth1Session

from poultry import consumers


logger = logging.getLogger(__name__)


def create_client(twitter_credentials):
        return OAuth1Session(
            twitter_credentials['consumer_key'],
            client_secret=twitter_credentials['consumer_secret'],
            resource_owner_key=twitter_credentials['access_token'],
            resource_owner_secret=twitter_credentials['access_token_secret'],
        )


class StreamProducer(Thread):
    """A producer of a tweet stream retrieved from twitter.

    :param target: A coroutine to which collected tweets are sent.
    :param twitter_credentials: A dictionary with `access_token`,
    `access_token_secret`, `consumer_key` and `consumer_secret`.
    :param follow: A list of user ids to follow.
    :param track: A list of phrases to track.
    :param locations: A list of coordinates to get.

    ..todo:: Parameter description!

    The default access level allows up to 400 track keywords, 5,000
    follow userids and 25 0.1-360 degree location boxes.
    https://dev.twitter.com/docs/streaming-api/methods

    """

    def __init__(self, target, twitter_credentials,
                 follow=None, track=None, locations=None, language=None,
                 url='https://stream.twitter.com/1.1/statuses/filter.json',
                 *args, **kwargs):
        super(StreamProducer, self).__init__(*args, **kwargs)

        self.target = target

        self.track = track if track is not None else []
        self.follow = follow if follow is not None else []
        self.locations = locations if locations is not None else []
        self.language = language if language is not None else []

        self.url = url
        self.client = create_client(twitter_credentials)

    def _run(self):
        target = self.target

        def _quote(items):
            # XXX It's not clear for me how the parameters have to be quote.
            if PY2:
                items = (i.encode('utf-8') for i in items)
            return ','.join(items)

        data = {
            p: _quote(getattr(self, p)) for p in 'track follow language'.split() if getattr(self, p)
        }

        locations = ','.join(str(f) for f in chain.from_iterable(chain.from_iterable(self.locations)))
        if locations:
            data['locations'] = locations

        logger.warn('The client is about to send a POST request.')
        response = self.client.post(
            self.url,
            data=data,
            stream=True,
            timeout=(3.05, 90.05),
        )
        logger.warn('The POST request is sent.')

        response.raise_for_status()

        line = None
        for line in response.iter_lines():
            target.send(line.decode('utf-8'))
        else:
            # XXX Should be changed to something meaningful
            raise EndOfStreamError(line)

    def run(self):
        try:
            while True:
                try:
                    self._run()
                # XXX implement meaningful reconnection strategy.
                #     https://dev.twitter.com/docs/streaming-apis/connecting#Reconnecting
                except requests.HTTPError:
                    logger.warn('An http error occurred. Reconnecting in a minute.', exc_info=True)
                    sleep(60)
                except EndOfStreamError:
                    logger.warn('The stream ended. Reconnecting in a minute.', exc_info=True)
                    sleep(60)
                except StopIteration:
                    logger.warn('The queue is full.')
                    break
        except KeyboardInterrupt:
            raise


class StreamConsumer(Thread):
    """A consumer of a stream of tweets."""
    def __init__(self, queue, target, *args, **kwargs):
        super(StreamConsumer, self).__init__(*args, **kwargs)

        self.queue = queue
        self.target = target

    def run(self):
        try:
            from_simple_queue(self.target, self.queue)
        except KeyboardInterrupt:
            pass


def from_simple_queue(target, queue):
    with consumers.closing(target):
        while True:
            item = queue.get()

            if item is StopIteration:
                break

            target.send(item)


def from_twitter_api(target, endpoint, config):
    """Consume tweets from a Streaming API endpoint."""
    endpoint_to_url = {
        'twitter://sample': 'https://stream.twitter.com/1.1/statuses/sample.json',
        'twitter://filter': 'https://stream.twitter.com/1.1/statuses/filter.json',
    }

    if endpoint == 'twitter://filter':
        filter_predicates = config.global_filter.predicates

        kwargs = {
            'follow': filter_predicates['follow'],
            'track': filter_predicates['track'],
            'locations': filter_predicates['locations'],
            'language': filter_predicates['language'],
        }
    else:
        kwargs = {}

    # The communication point of the consumer and producer processes.
    queue = Queue(maxsize=100)

    # Create the producer first to be sure that it exists before creating and
    # starting the consumer.
    producer = StreamProducer(
        twitter_credentials=dict(config.items('twitter')),
        target=consumers.to_simple_queue(queue),
        url=endpoint_to_url[endpoint],
        **kwargs
    )

    # start the consumer
    consumer = StreamConsumer(queue, target)
    consumer.start()

    producer.start()
    try:
        producer.join()
    finally:
        # Tell the consumer to stop
        try:
            queue.put(StopIteration, block=False)
        except Full:
            pass
        else:
            consumer.join()


class EndOfStreamError(IOError):
    """Twitter streams are supposed to be infinite.

    The exception should be thrown, if for any reason the stream has ended.

    """
