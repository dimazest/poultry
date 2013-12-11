from datetime import datetime as DT

import pytest

from poultry import consumers
from poultry.tweet import Tweet
from poultry.merge import merge, _merger, _conductor, _tagger

from .test_consumers import consume_iterable, to_list


@pytest.fixture
def a():
    return 'a', 'cc', 'd'


@pytest.fixture
def b():
    return 'aa', 'c'


def test_merge(a, b):
    result = []
    sink = to_list(result)

    pipes = (
        (lambda target: consume_iterable(target, a), sink),
        (lambda target: consume_iterable(target, b), sink),
    )

    merge(pipes)

    assert result == ['a', 'aa', 'c', 'cc', 'd']


def test_merge_different_targets(a, b):
    result_a = []
    sink_a = to_list(result_a)
    result_b = []
    sink_b = to_list(result_b)

    pipes = (
        (lambda target: consume_iterable(target, a), sink_a),
        (lambda target: consume_iterable(target, b), sink_b),
    )

    merge(pipes)

    assert result_a == list(a)
    assert result_b == list(b)


def test_merge_tweets(tweets):
    a = tweets[:1] * 3
    b = tweets[1:2] * 4
    result = []
    sink = to_list(result)

    def S(source):
        t = lambda target: consume_iterable(
            consumers.to_tweet(target),
            source
        )

        return t

    pipes = (S(a), sink), (S(b), sink)
    merge(pipes, provider=lambda current: current.created_at)

    assert [t.created_at for t in result] == [DT(2012, 4, 13, 13, 55, 2),
                                              DT(2012, 4, 13, 13, 55, 2),
                                              DT(2012, 4, 13, 13, 55, 2),
                                              DT(2012, 4, 26, 7, 35, 39),
                                              DT(2012, 4, 26, 7, 35, 39),
                                              DT(2012, 4, 26, 7, 35, 39),
                                              DT(2012, 4, 26, 7, 35, 39),
                                              ]


@pytest.mark.xfail(reason='If the last source is empty, the priority information is lost!')
def test_merge_empty():
    result = []
    sink = to_list(result)

    pipes = (
        (lambda target: consume_iterable(target, []), sink),
        (lambda target: consume_iterable(target, []), sink),
    )

    merge(pipes)

    assert not result


@pytest.mark.xfail(reason='If the last source is empty, the priority information is lost!')
def test_merge_empty_one():
    result = []
    sink = to_list(result)

    pipes = (
        (lambda target: consume_iterable(target, [1, 2]), sink),
        (lambda target: consume_iterable(target, []), sink),
    )

    merge(pipes)

    assert result == [1, 2]


def test_merge_mutator(a, b):
    result = []
    sink = to_list(result)

    def S(source):
        return lambda target: consume_iterable(
            consumers.mutate(
                target,
                mutator=lambda i, _: i.upper(),
            ),
            source,
        )

    pipes = (S(a), sink), (S(b), sink)
    merge(pipes)

    assert result == ['A', 'AA', 'C', 'CC', 'D']


def test_merge_mutator_tweets(tweets):
    t1, t2, t3 = map(Tweet, tweets)

    result = []
    sink = to_list(result)
    start = DT(2000, 1, 1, 0, 0, 0)

    def mutator(i, f):
        delta = i.orig_created_at - f.orig_created_at
        i.created_at = start + delta
        return i

    def S(source):
        return lambda target: consume_iterable(
            consumers.mutate(
                target,
                mutator=mutator,
            ),
            source,
        )

    pipes = (S([t1, t2]), sink), (S([t3]), sink)
    merge(
        pipes,
        provider=lambda current: current.created_at,
    )

    assert result == [t1, t3, t2]
    assert [t.created_at for t in result] == [DT(2000, 1, 1, 0, 0),
                                              DT(2000, 1, 1, 0, 0),
                                              DT(2000, 1, 13, 17, 40, 37),
                                              ]


def test_conductor(a, b):
    result = []
    sink = to_list(result)

    merger = _merger({'A': sink,
                      'B': sink,
                      })

    tagged_sources = {'A': consume_iterable(_tagger(merger, 'A'), a),
                      'B': consume_iterable(_tagger(merger, 'B'), b),
                      }

    conductor = _conductor(merger, tagged_sources)

    # This ends the first step, all sources are applied.
    item, priority = next(conductor)  # B
    assert item == 'a'
    priority = ['A', 'B']
    assert result == ['a']

    item, priority = next(conductor)
    assert item == 'aa'
    assert priority == ['B', 'A']
    assert result == ['a', 'aa']

    item, priority = next(conductor)
    assert item == 'c'
    assert priority == ['B', 'A']
    assert result == ['a', 'aa', 'c']

    item, priority = next(conductor)
    assert item == 'd'
    assert priority == ['A']
    assert result == ['a', 'aa', 'c', 'cc', 'd']

    with pytest.raises(StopIteration):
        next(conductor)
    assert result == ['a', 'aa', 'c', 'cc', 'd']


def test_conductor2(a, b):
    result = []
    sink = to_list(result)

    merger = _merger({'A': sink,
                      'B': sink,
                      })

    tagged_sources = {'A': consume_iterable(_tagger(merger, 'A'), a),
                      'B': consume_iterable(_tagger(merger, 'B'), b),
                      }

    conductor = _conductor(merger, tagged_sources)

    for _ in conductor:
        pass

    assert result == ['a', 'aa', 'c', 'cc', 'd']


@pytest.mark.xfail(reason='If the last source is empty, the priority information is lost!')
def test_conductor_empty():
    a = ['a']
    b = []
    result = []
    sink = to_list(result)

    merger = _merger({'A': sink,
                      'B': sink,
                      })

    tagged_sources = {'A': consume_iterable(_tagger(merger, 'A'), a),
                      'B': consume_iterable(_tagger(merger, 'B'), b),
                      }

    conductor = _conductor(merger, tagged_sources)

    next(conductor)

    with pytest.raises(StopIteration):
        next(conductor)

    assert result == ['a']


def test_merger(a, b):
    result_a = []
    sink_a = to_list(result_a)

    result_b = []
    sink_b = to_list(result_b)

    merger = _merger({'A': sink_a, 'B': sink_b})

    source_a = consume_iterable(_tagger(merger, 'A'), a)
    source_b = consume_iterable(_tagger(merger, 'B'), b)

    assert next(source_a) is None
    #   a  | cc   d
    # | aa   c

    item, source_priority = next(source_b)
    #      | cc   d
    #   aa | c
    assert source_priority == ['A', 'B']
    assert result_a == ['a']
    assert result_b == []
    assert item == 'a'

    item, source_priority = next(source_a)
    #        cc | d
    #      | c
    assert source_priority == ['B', 'A']
    assert result_a == ['a']
    assert result_b == ['aa']
    assert item == 'aa'

    item, source_priority = next(source_b)
    #        cc | d
    #           |
    assert source_priority == ['B', 'A']
    assert result_a == ['a']
    assert result_b == ['aa', 'c']
    assert item == 'c'

    with pytest.raises(StopIteration):
        next(source_b)
    #           | d
    #
    assert result_a == ['a', 'cc']
    assert result_b == ['aa', 'c']

    item, source_priority = next(source_a)
    #               |
    #
    assert source_priority == ['A']
    assert result_a == ['a', 'cc', 'd']
    assert result_b == ['aa', 'c']

    with pytest.raises(StopIteration):
        next(source_a)
    #
    #
    assert result_a == ['a', 'cc', 'd']
    assert result_b == ['aa', 'c']

    with pytest.raises(StopIteration):
        next(merger)


def test_merger2():
    a = 'aaa',
    b = 'aa',
    c = 'a'
    result = []
    sink = to_list(result)

    merger = _merger({'A': sink,
                      'B': sink,
                      'C': sink,
                      })

    source_a = consume_iterable(_tagger(merger, 'A'), a)
    source_b = consume_iterable(_tagger(merger, 'B'), b)
    source_c = consume_iterable(_tagger(merger, 'C'), c)

    assert next(source_a) is None

    assert next(source_b) is None

    assert next(source_c) == ('a', ['C', 'B', 'A'])

    with pytest.raises(StopIteration):
        next(source_c)

    with pytest.raises(StopIteration):
        next(source_b)

    with pytest.raises(StopIteration):
        next(source_a)

    assert result == ['a', 'aa', 'aaa']
