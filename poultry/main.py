"""Commands for manipulating local tweet collection."""

import sys
import codecs
import datetime

from poultry import consumers, producers, options
from poultry.merge import merge as merge_
from poultry.streaming import dispatcher as streaming_dispatcher

# Always write utf8
# _orig_stdout = sys.stdout
# sys.stdout = codecs.getwriter('utf8')(sys.stdout)


global_options = (
    ('i', 'input_dir', '', 'Directory with tweet files'),
)
dispatcher = options.Dispatcher(*global_options)
command = dispatcher.command

dispatcher.nest(
    'streaming',
    streaming_dispatcher,
    'Access the Twitter straming API.',
)


@command()
def group(input_dir,
          file_name_template=('t', '%Y-%m-%d-%H.gz', ''),
          ):
    """Group tweets to files by date according to the template."""
    producers.from_stdin_or_dir(consumers.to_tweet(consumers.group(file_name_template)), input_dir)


@command()
def show(input_dir):
    """Print tweets in human readable form."""
    producers.from_stdin_or_dir(consumers.to_tweet(consumers.show()), input_dir)


@command()
def select(input_dir):
    """Print tweets as it is returned by the Twitter streaming API."""
    producers.from_stdin_or_dir(consumers.print_(), input_dir)


@command()
def pprint(input_dir):
    """Pretty print tweet's json representation."""
    producers.from_stdin_or_dir(consumers.to_tweet(consumers.pprint()), input_dir)


@command()
def filter(input_dir, config):
    """Filter the tweets to files by filtering predicates defined in the configuration file."""
    dustbin_template = config.dustbin_template
    dustbin = consumers.group(dustbin_template) if dustbin_template is not None else None

    streams = tuple((consumers.group(f.split_template),
                     lambda c, _, f=f: c.filter(**f.predicates),
                     ) for f in config.filters)
    target = consumers.filter(streams, dustbin)

    producers.from_stdin_or_dir(consumers.to_tweet(target), input_dir)


@command()
def uniq(input_dir):
    """Omit repeated tweets."""
    producers.from_stdin_or_dir(consumers.to_tweet(consumers.uniq(consumers.select())),
                                input_dir)


@command()
def timeline(input_dir, window=('w', '%Y-%m-%d-%H', '')):
    """Count the number of tweets per window."""

    producers.from_stdin_or_dir(
        consumers.to_tweet(
            consumers.timeline(
                window=window,
                target=consumers.counter_printer(sys.stdout),
            ),
        ),
        input_dir,
    )


@command()
def merge(merge, input_dir, config):
    merge = config.merge(merge)

    def mutator(c, f, start=datetime.datetime(2000, 1, 1, 0, 0)):
        delta = c.orig_created_at - f.orig_created_at
        c.created_at = start + delta

        return c

    def Source(input_):
        return lambda target: producers.consume_stdin_or_dir(
            consumers.to_tweet(
                consumers.mutate(
                    target,
                    mutator=mutator,
                ),
            ),
            input_,
        )

    def provider(current):
        return current.created_at

    sink = consumers.select()
    pipeline = ((Source(input_), sink) for input_ in merge.inputs)

    merge_(pipeline, provider=provider)


