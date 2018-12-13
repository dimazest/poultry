from __future__ import print_function

import gzip
import logging
import sys
import time
import csv

try:
    from Queue import Full
except ImportError:
    from queue import Full

from collections import OrderedDict, Counter
from contextlib import contextmanager
from itertools import chain
from pprint import pprint as _pprint

from poultry.tweet import Tweet, TweetValueError


logger = logging.getLogger(__name__)


def consumer(func):
    """A decorator function that takes care of starting a coroutine automatically on call.

    See http://www.dabeaz.com/generators/ for more details.

    """
    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        next(cr)
        return cr
    return start


@consumer
def nop(*args, **kwargs):
    item = None
    while True:
        item = yield item


@consumer
def show(output=None, template=u'{t}\n'):
    """Print tweet text and meta information."""
    if output is None:
        output=sys.stdout
    while True:
        try:
            print(template.format(t=(yield)), file=output)
        except KeyError:
            pass


@consumer
def print_(output=None, template=u'{}\n'):
    """Print stripped items."""
    if output is None:
        output = sys.stdout
    while True:
        item = yield
        print(template.format((item).strip('\n')), file=output)


@consumer
def pprint():
    """Pretty print tweet's json object."""
    while True:
        tweet = (yield)
        _pprint(tweet.parsed)


@consumer
def print_text(output=None):
    """Print only tweet's text."""
    if output is None:
        output = sys.stdout
    while True:
        tweet = yield
        print(tweet.text.replace(u'\n', u' '), file=output)


@consumer
def counter_printer(output=None):
    if output is None:
        output = sys.stdout
    while True:
        counter = yield

        for key, value in sorted(counter.items()):
            output.write('{} {}\n'.format(key, value))


@consumer
def to_tweet(target):
    """Convert the input items to tweets."""
    result = None

    with closing(target):
        while True:
            item = yield result

            try:
                tweet = Tweet(item)
            except TweetValueError:
                result = SendNext
            else:
                result = target.send(tweet)


@consumer
def group(
    file_name_template='%Y-%m-%d-%H.gz',
    max_open_files=1,
    mode='a',
):
    """Group tweets to files by date according to the file_name_template."""
    files = OrderedDict()

    try:
        while True:
            tweet = yield

            created_at = tweet.created_at.strftime(file_name_template)

            try:
                f = files[created_at]
            except KeyError:
                print(created_at)

                if len(files) > max_open_files:
                    files = OrderedDict(sorted(files.items()))

                    _, first = files.popitem(last=False)
                    first.close()

                f = gzip.open(created_at, '{}t'.format(mode))
                files[created_at] = f

            f.write("{t}\n".format(t=tweet.raw))

    finally:
        for f in files.values():
            f.close()


@consumer
def filter(streams, dustbin=None, send_to_all=True):
    """Filter items to flows by filtering predicates.

    :param streams: sequence of `(target, predicate)` pairs.

    :param send_to_all: if set to `False` then in case an item
                        satisfies several predicates it will be sent
                        to only one.

    `predicate` is a function: current, first --> Bool

    """
    def update_all_targets():
        all_targets = [t for t, _ in streams]
        if dustbin is not None:
            all_targets.append(dustbin)
        return all_targets

    all_targets = update_all_targets()

    with closing(mutable_targets=all_targets):
        current = first = yield

        while True:
            sent = False
            for target, predicate in streams:
                if predicate(current, first):
                    target.send(current)
                    sent = True

                    if not send_to_all:
                        break

            if dustbin is not None and not sent:
                dustbin.send(current)

            current = yield
            all_targets = update_all_targets()


@consumer
def uniq(target, seen_ids=None):
    """Omit repeated tweets."""
    seen_ids = set(seen_ids) if seen_ids is not None else set()

    with closing(target):
        while True:
            tweet = yield
            id_ = tweet.id

            if id_ not in seen_ids:
                seen_ids.add(id_)
                target.send(tweet)


@consumer
def split(*targets):
    """Send the input items to each target."""
    with closing(*targets):
        while True:
            item = yield

            for target in targets:
                target.send(item)


@consumer
def to_simple_queue(queue):
    """Put items to a simple queue."""
    while True:
        item = yield
        size = queue.qsize()
        if size > 10:
            logger.warn('Queue size is %s.', size)
        try:
            queue.put(item, timeout=1)
        except Full:
            break


@consumer
def count(*counters, **kwargs):
    """Universal element counter.

    :param counters: counters to update.

    :param target: an optional target to which the cached local counts
                   are sent per each batch.

    :param provider: a function which provides elements that have to
                     be counted given an input item.

    """
    provider = kwargs.get('provider', None)
    target = kwargs.get('target', None)
    targets = [target] if target else []

    if provider is None:
        provider = lambda x: [x]

    if not counters:
        counters = Counter(),

    with closing(*targets):
        while True:
            with lazy_counter(*counters, target=target) as c:
                while True:
                    item = yield
                    c.update(provider(item))


def count_tokens(*counters, **kwargs):
    """Count tokens in a tweet."""
    if kwargs.pop('distinguish_hashtags', True):
        kwargs['provider'] = lambda tweet: chain(tweet.tokens,
                                                 (u'#{}'.format(h) for h in tweet.hashtags),
                                                 )
    else:
        kwargs['provider'] = lambda tweet: chain(tweet.tokens, tweet.hashtags)

    return count(*counters, **kwargs)


def timeline(*counters, **kwargs):
    """Count tweet's creation time."""
    window = kwargs.pop('window', '%Y-%m-%d-%H')
    kwargs['provider'] = lambda tweet: [tweet.created_at.strftime(window)]

    return count(*counters, **kwargs)


@consumer
def batch(target, flow_name=None, splitter=None):
    """Batch a stream of tweets to chunks defined by `splitter`.

    :param target: a coroutine tweets are sent to.
    :param flow_name: an optional flow name, which is used mainly for logging.
    :param splitter: a function which decides whether a new batch has started.
    :type splitter: (datetime, Tweet) --> bool

    """
    batch_size = 0

    if splitter is None:
        def splitter(current, first):
            return current != first

    with closing(target):

        current = first = yield

        while True:
            batch_size += 1

            if splitter(current, first):
                logger.debug('Sending a batch of %s items. (%s) '
                             'first: %s current: %s',
                             batch_size, flow_name, first, current)

                target.throw(BatchEndException(current, batch_size))
                batch_size = 0
                first = current

            result = target.send(current)
            current = yield result


@contextmanager
def lazy_counter(*counters, **kwargs):
    """Delay the update of the counter."""
    target = kwargs.get('target')

    local_counter = Counter()
    try:
        yield local_counter
    except BatchEndException:
        pass
    finally:
        if target:
            target.send(local_counter)

        for counter in counters:
            counter.update(local_counter)


@consumer
def delay(target, speedup=100.0, max_delay=2):
    last = None

    with closing(target):
        while True:
            tweet = yield

            if speedup and last is not None and last < tweet.created_at:
                delay = tweet.created_at - last
                delay = delay.total_seconds() / speedup
                delay = min([delay, max_delay])

                time.sleep(delay)

            last = tweet.created_at
            target.send(tweet.raw)


@consumer
def mutate(target, mutator=None):
    if mutator is None:
        def mutator(current, first):
            return current

    result = None

    with closing(target):
        first = current = yield result

        while True:
            result = target.send(mutator(current, first))
            current = yield result


@contextmanager
def closing(*targets, **kwargs):
    """Throw exceptions to targets and close targets on exit."""
    mutable_targets = kwargs.get('mutable_targets', [])
    try:
        yield
    finally:
        for target in chain(targets, mutable_targets):
            target.close()


class PropogatedException(Exception):
    """"""


class BatchEndException(PropogatedException):
    """
    Is thrown by batch() to the target generator on the end of the
    current batch.
    """

    def __init__(self, last_item, batch_size):
        self.last_item = last_item
        self.batch_size = batch_size


class SendNext(object):
    """
    Returned a consumer if the sent value is ignored, and the next value
    should be sent immediately.
    """


@consumer
def extract_retweets(target):
    """Extract retweets.

    Note that it doesn't keep track of duplicates and the order.

    """

    while True:
        raw_tweet = yield
        tweet = Tweet(raw_tweet)

        retweeted_status = tweet.parsed.get('retweeted_status', None)
        if retweeted_status:
            target.send(retweeted_status)

        target.send(raw_tweet)


@consumer
def print_media(output=None):
    """Print media items."""
    if output is None:
        output = sys.stdout

    field_names = 'tweet_id', 'index', 'media_id', 'type', 'media_url'
    writer = csv.DictWriter(output, fieldnames=field_names)

    while True:
        tweet = yield
        media = tweet.parsed.get('extended_entities', {}).get('media', [])

        for i, m in enumerate(media):
            writer.writerow(
                {
                    'tweet_id': tweet.id,
                    'index': i,
                    'media_id': m['id'],
                    'type': m['type'],
                    'media_url': m['media_url'],
                }
            )
