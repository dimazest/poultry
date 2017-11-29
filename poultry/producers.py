import fileinput
import contextlib

from poultry import consumers
from poultry.stream import from_twitter_api
from poultry.tweet import Tweet, TweetValueError
from poultry.utils import get_file_names


def consume_stream(target, input_dir=None):
    """Read lines from the standard input or files in a directory.

    Behaves as a generator, should receive a .send() call to send a
    line to the target.

    """
    file_names = get_file_names(input_dir) if input_dir else []

    with contextlib.closing(fileinput.FileInput(file_names, openhook=fileinput.hook_compressed)) as lines:
        targets = [target] if target is not None else []
        with consumers.closing(*targets):

            for line in lines:
                if not line.strip():
                    continue

                if isinstance(line, bytes):
                    line = line.decode('utf-8')

                if target is not None:
                    result = target.send(line)
                    if result is not consumers.SendNext:
                        yield result
                else:
                    yield line


def readline_dir(input_dir, extract_retweets=False, mark_extracted=False):
    """Read a tweet collection directory.

    :param str input_dir: the patht to the directory

    :return: an iterable of Tweet objects

    """
    for l in consume_stream(target=None, input_dir=input_dir):
        try:
            tweet = Tweet(l)
        except TweetValueError:
            continue
        else:

            # TODO: this duplicates consumers.extract_retweets.
            retweeted_status = tweet.parsed.get('retweeted_status', None)
            if extract_retweets and retweeted_status:
                    yield Tweet(retweeted_status)
            yield tweet


def from_stream(target, source=None, config=None, extract_retweets=False):
    """Send lines from the standard input, the input directory or the Twitter Streaming API.

    :param target: a generator to which the read lines are sent.
    :param source: the path to a directory with tweet files.
    :param config: the config file
    :param extract_retweets: Extract retweets from the tweets.

    """
    if extract_retweets:
        target = consumers.extract_retweets(target)

    if source in ('twitter://sample', 'twitter://filter'):
        consumer = from_twitter_api(target, source, config)
    else:
        consumer = consume_stream(target, source)

        try:
            while True:
                next(consumer)
        except StopIteration:
            pass
