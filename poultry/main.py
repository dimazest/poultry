"""Commands for manipulating local tweet collection."""

import sys

from poultry import consumers, options

dispatcher = options.Dispatcher()
command = dispatcher.command

# Making conda_build happy
dispatch = dispatcher.dispatch


@command()
def group(producer,
          file_name_template=('t', '%Y-%m-%d-%H.gz', ''),
          ):
    """Group tweets to files by date according to the template."""
    producer(consumers.to_tweet(consumers.group(file_name_template)))


@command()
def show(
    producer,
    template=('t', u'{t}\n', 'Message template.'),

):
    """Print tweets in human readable form."""
    producer(consumers.to_tweet(consumers.show(template=template)))


@command()
def select(producer, output):
    """Print tweets as it is returned by the Twitter streaming API."""
    producer(consumers.print_(output=output))


@command()
def text(
    producer,
    output,
):
    """Print only tweet's text.

    It replaces the new line symbol (\\n) with a space.

    """
    producer(consumers.to_tweet(consumers.print_text(output=output)))


@command()
def pprint(producer):
    """Pretty print tweet's json representation."""
    producer(consumers.to_tweet(consumers.pprint()))


@command()
def filter(
    producer, config, output,
    mode=('', u'a', 'The mode to open the files, `a` to append and `w` to rewrite.'),
    filters=('', [], 'The filters to use.'),
):
    """Filter the tweets to files by filtering predicates defined in the configuration file."""
    dustbin_template = config.dustbin_template
    dustbin = consumers.group(dustbin_template) if dustbin_template is not None else None

    filters_to_include = config.filters
    if filters:
        filters_to_include = (f for f in config.filters if f.name in filters)

    streams = tuple(
        (
            consumers.group(f.split_template, mode=mode)
            if f.split_template != '--'
            else consumers.show(output=output, template='{t.raw}'),
            lambda c, _, f=f: c.filter(**f.predicates),
        )
        for f in filters_to_include
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


@command()
def media(producer, output):
    """Retrieve media urls."""
    producer(
        consumers.to_tweet(
            consumers.print_media(output=output)
        )
    )
