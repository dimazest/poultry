import urllib
import logging
from time import sleep
from itertools import chain
from multiprocessing import Process
from multiprocessing.queues import SimpleQueue

import requests
from requests_oauthlib import OAuth1Session

from poultry import consumers


logger = logging.getLogger(__name__)


class StreamProducer(Process):
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
                 follow=None, track=None, locations=None,
                 url='https://stream.twitter.com/1.1/statuses/filter.json',
                 *args, **kwargs):
        super(StreamProducer, self).__init__(*args, **kwargs)

        self.target = target

        self.track = track if track is not None else []
        self.follow = follow if follow is not None else []
        self.locations = locations if locations is not None else []
        self.url = url

        self.client = OAuth1Session(
            twitter_credentials['consumer_key'],
            client_secret=twitter_credentials['consumer_secret'],
            resource_owner_key=twitter_credentials['access_token'],
            resource_owner_secret=twitter_credentials['access_token_secret'],
        )

    def _run(self):
        target = self.target

        def quote(items):
            # XXX It's not clear for me how the parameters have to be
            #     quote.
            items = (i.encode('utf-8') for i in items)
            return urllib.quote(','.join(items), safe=', ')

        data = {
            p: quote(getattr(self, p)) for p in 'track follow'.split() if getattr(self, p)
        }

        locations = ','.join(str(f) for f in chain.from_iterable(chain.from_iterable(self.locations)))
        if locations:
            data['locations'] = locations
        response = self.client.post(self.url, data=data, stream=True)

        response.raise_for_status()

        line = None
        for line in response.iter_lines():
            target.send(line)
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
                    logger.warn('An http error occurred. Reconnecting...', exc_info=True)
                    sleep(10)
                except EndOfStreamError:
                    logger.warn('The stream ended. Reconnecting...', exc_info=True)
                    sleep(60)
        except KeyboardInterrupt:
            pass


class StreamConsumer(Process):
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
        }
    else:
        kwargs = {}

    # The communication point of the consumer and producer processes.
    queue = SimpleQueue()

    # Start the consumer first
    consumer = StreamConsumer(queue, target)
    consumer.start()

    # then the producer.
    producer = StreamProducer(
        twitter_credentials=dict(config.items('twitter')),
        target=consumers.to_simple_queue(queue),
        url=endpoint_to_url[endpoint],
        **kwargs
    )

    producer.start()

    try:
        producer.join()
    finally:
        queue.put(StopIteration)
        consumer.join()


class EndOfStreamError(IOError):
    """Twitter streams are supposed to be infinite.

    The exception should be thrown, if for any reason the stream has ended.

    """
