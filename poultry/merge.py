import heapq
import logging

from poultry.consumers import closing, consumer


logger = logging.getLogger(__name__)


def merge(pipes, provider=None):
    m = _merge(pipes, provider)

    while True:
        try:
            next(m)
        except StopIteration:
            break


@consumer
def _merge(pipes, provider=None):
    epipes = list(enumerate(pipes))

    merger = _merger(
        {tag: t for tag, (_, t) in epipes},
        provider=provider,
    )

    tagged_sources = {tag: s(_tagger(merger, tag)) for tag, (s, _) in epipes}

    conductor = _conductor(merger, tagged_sources)

    while True:
        r = next(conductor)
        if r is not None:
            item, _ = r
            yield item


@consumer
def _conductor(merger, tagged_sources):
    result = None

    with closing(merger):
        # The first step: push each target once.
        for source in tagged_sources.values():
            try:
                result = next(source)
                if result is None:
                    yield
            except StopIteration:
                pass

        item, source_priority = result

        # The second step: push targets as merge requests.
        while True:
            yield item, source_priority

            while source_priority:
                tag = source_priority.pop(0)
                try:
                    item, source_priority = next(tagged_sources[tag])
                except StopIteration:
                    pass
                else:
                    break
            else:
                return


@consumer
def _merger(tagged_targets, provider=None):
    if provider is None:
        def provider(current):
            return current

    targets = set(tagged_targets.values())
    with closing(*targets):

        items = []
        heapq.heapify(items)

        applied_sources = set()
        closed_sources = set()
        all_sources = set(tagged_targets)

        try:
            # First step: get at least one item from each source
            while applied_sources != all_sources:
                try:
                    tag, item = yield
                except TaggedGeneratorExit as e:
                    applied_sources.add(e.tag)
                    closed_sources.add(e.tag)
                else:
                    heapq.heappush(items, (provider(item), tag, item))
                    applied_sources.add(tag)

            # Second step: push forward the smallest item to its target
            # and push back to conductor the priorities of the sources.
            while closed_sources != applied_sources:
                priorities = [i[1] for i in sorted(items)]

                _, tag, item = heapq.heappop(items)

                result = tagged_targets[tag].send(item)

                try:
                    tag, item = yield result, priorities
                except TaggedGeneratorExit as e:
                    closed_sources.add(e.tag)
                    logger.debug('%s is closed', tag)
                else:
                    heapq.heappush(items, (provider(item), tag, item))

        finally:
            for _, tag, item in sorted(items):
                tagged_targets[tag].send(item)


@consumer
def _tagger(target, tag):
    """Send to the target pairs (tag, item)."""
    # Target is not being closed!
    result = None

    while True:
        try:
            item = yield result
        except GeneratorExit as e:
            target.throw(TaggedGeneratorExit(tag))
            raise e
        else:
            result = target.send((tag, item))


class TaggedGeneratorExit(Exception):
    def __init__(self, tag, *args, **kwargs):
        super(TaggedGeneratorExit, self).__init__(*args, **kwargs)

        self.tag = tag
