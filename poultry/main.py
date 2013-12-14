"""Commands for manipulating local tweet collection."""

import sys

from poultry import consumers, options

dispatcher = options.Dispatcher()
command = dispatcher.command


@command()
def group(producer,
          file_name_template=('t', '%Y-%m-%d-%H.gz', ''),
          ):
    """Group tweets to files by date according to the template."""
    producer(consumers.to_tweet(consumers.group(file_name_template)))


@command()
def show(producer):
    """Print tweets in human readable form."""
    producer(consumers.to_tweet(consumers.show()))


@command()
def select(producer):
    """Print tweets as it is returned by the Twitter streaming API."""
    producer(consumers.print_())


@command()
def text(producer):
    """Print only tweet's text.

    It replaces the new line symbol (`\n`) with a space.

    """
    producer(consumers.to_tweet(consumers.print_text()))


@command()
def pprint(producer):
    """Pretty print tweet's json representation."""
    producer(consumers.to_tweet(consumers.pprint()))


@command()
def filter(producer, config):
    """Filter the tweets to files by filtering predicates defined in the configuration file."""
    dustbin_template = config.dustbin_template
    dustbin = consumers.group(dustbin_template) if dustbin_template is not None else None

    streams = tuple(
        (
            consumers.group(f.split_template),
            lambda c, _, f=f: c.filter(**f.predicates),
        )
        for f in config.filters
    )
    target = consumers.filter(streams, dustbin)

    producer(consumers.to_tweet(target))


@command()
def uniq(producer):
    """Omit repeated tweets."""
    producer(
        consumers.to_tweet(
            consumers.uniq(consumers.select())
        ),
    )


@command()
def timeline(producer, window=('w', '%Y-%m-%d-%H', '')):
    """Count the number of tweets per window."""

    producer(
        consumers.to_tweet(
            consumers.timeline(
                window=window,
                target=consumers.counter_printer(sys.stdout),
            ),
        ),
    )
