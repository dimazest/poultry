try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from collections import Counter

try:
    from multiprocessing import SimpleQueue
except ImportError:
    from multiprocessing.queues import SimpleQueue

from poultry import consumers
from poultry.tweet import Tweet

import pytest
from pytest import raises


def from_iterable(target, iterable):
    consumer = consume_iterable(target, iterable)

    try:
        while True:
            next(consumer)
    except StopIteration:
        pass


def consume_iterable(target, iterable):
    try:
        for item in iterable:
            result = yield target.send(item)
            assert result is not consumers.SendNext
    finally:
        target.close()


@consumers.consumer
def to_list(list_):
    item = None
    while True:
        item = yield item
        list_.append(item)


def test_basic(tweets):
    result = []
    from_iterable(consumers.to_tweet(to_list(result)), tweets)

    assert len(result) == 3
    assert all(isinstance(t, Tweet) for t in result)


def test_bad_json():
    result = []
    from_iterable(consumers.to_tweet(to_list(result)), ['not valid JSON'])

    assert not result


def test_filter(tweets):
    pinkpop = []
    dimazest = []
    dustbin = []

    streams = tuple((to_list(l),
                     lambda c, f, p=p: c.filter(**p)
                     ) for l, p in [(pinkpop, {'follow': [],
                                               'track': ['pinkpop'],
                                               'locations': [],
                                               },
                                     ),
                                    (dimazest, {'follow': [10868922],
                                                'track': [],
                                                'locations': [],
                                                },
                                     ),
                                    ]
                    )

    target = consumers.filter(streams, to_list(dustbin))
    from_iterable(consumers.to_tweet(target), tweets)

    assert len(pinkpop) == 1
    assert pinkpop[0].id == 190800262909276162

    assert len(dimazest) == 3

    assert not dustbin


def test_filter_dustbin(tweets):
    result = []
    dustbin = []

    streams = ((to_list(result), lambda c, f: c.filter(follow=[-1000])), )

    target = consumers.filter(streams, to_list(dustbin))
    from_iterable(consumers.to_tweet(target), tweets)

    assert not result
    assert len(dustbin) == 3


def test_filter_numbers():
    items = 4, 2, 3, 4, 5, 9, 0, 2, 8
    result = []

    streams = ((to_list(result), lambda c, f: c > f),)

    from_iterable(consumers.filter(streams), items)

    assert result == [5, 9, 8]


def test_uniq(tweets):
    result = []
    from_iterable(consumers.to_tweet(consumers.uniq(to_list(result))), tweets * 20)

    assert len(result)


def test_closing():
    sink = to_list([])
    target = consumers.to_tweet(sink)

    target.close()

    with raises(StopIteration):
        sink.send('Sink is expected to be closed too.')


@pytest.mark.xfail(run=False)
def test_exception_propagation():

    class ThrownException(consumers.PropogatedException):
        pass

    class SinkException(Exception):
        pass

    @consumers.consumer
    def SpecialTarget(target):
        try:
            with consumers.closing(target):
                while True:
                    item = yield
                    target.send(item)
        except ThrownException:
            pass

    @consumers.consumer
    def SpecialSink():
        try:
            yield
        except ThrownException:
            raise SinkException()

    sink = SpecialSink()
    target = SpecialTarget(sink)

    with raises(SinkException):
        target.throw(ThrownException('Sink has to catch it'))

    with raises(StopIteration):
        sink.send("It's closed!")


def test_split():
    input_ = [1, 2, 3, 3, 4]
    result_a = []
    result_b = []

    from_iterable(consumers.split(to_list(result_a),
                                  to_list(result_b),
                                  ), input_)

    assert result_a == result_b == input_


def test_simple_queue():
    q = SimpleQueue()
    input_ = [1, 2, 3, 4, 5, 6]
    from_iterable(consumers.to_simple_queue(q), input_)

    for i in input_:
        o = q.get()
        assert o == i

    assert q.empty()


def test_batch(tweets):

    @consumers.consumer
    def batch_end_consumer():
        batch_end_consumer.batches = 0
        while True:
            try:
                yield
            except consumers.BatchEndException:
                batch_end_consumer.batches += 1

    from_iterable(consumers.to_tweet(consumers.batch(batch_end_consumer())), tweets)

    assert batch_end_consumer.batches == 2


def test_count_output():
    output = StringIO()
    from_iterable(consumers.count(output=output,
                                  target=consumers.counter_printer(output),
                                  ),
                  [1, 1, 2, 3, 4, 4, 4, 4])

    result = output.getvalue()
    assert result == '1 2\n2 1\n3 1\n4 4\n'


def test_count_batch(tweets):
    counter = Counter()

    from_iterable(
        consumers.to_tweet(
            consumers.batch(
                consumers.timeline(counter)
            ),
        ),
        tweets,
    )

    assert counter == Counter(
        {
            '2012-05-12-09': 1,
            '2012-04-26-07': 1,
            '2012-04-13-13': 1,
        }
    )


def test_count_timeline(tweets):
    counter = Counter()

    from_iterable(
        consumers.to_tweet(
            consumers.timeline(counter),
        ),
        tweets,
    )

    assert counter == Counter(
        {
            '2012-05-12-09': 1,
            '2012-04-26-07': 1,
            '2012-04-13-13': 1,
        }
    )


def test_count_tokens(tweets):
    counter = Counter()

    from_iterable(
        consumers.to_tweet(
            consumers.count_tokens(counter),
        ),
        tweets,
    )

    assert counter == Counter(
        {
            u'paaspop': 1,
            u'all': 1,
            u'pinkpop': 1,
            u'thats': 1,
            u'pedropicopop': 1,
            u'use': 1,
            u'here': 1,
            u'pukkelpop': 1,
            u'prilpop': 1,
            u'fun': 1,
            u'come': 1,
            u'#pygrunn': 1,
            u'#pp12': 1,
        }
    )
