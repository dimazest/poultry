from poultry.tweet import Tweet, TweetValueError, intersect, Coordinates

import pytest
from pytest import raises


def test_init(tweets):
    t = tweets[0]

    tweet = Tweet(t)
    assert tweet.id == 190800262909276162


def test_not_json():
    with raises(TweetValueError):
        Tweet('a b c')


def test_invalid_schema():
    with raises(TweetValueError):
        Tweet('{"1": 2}')

    with raises(TweetValueError):
        Tweet('[1, 2, 3]')

    with raises(TweetValueError):
        Tweet('1')


def test_filter_follow(tweets):
    one, two, three = (Tweet(t) for t in tweets[:3])

    assert all(t.filter(follow=[10868922]) for t in (one, two, three))


@pytest.mark.parametrize("a,b,expected", [
    (((0, 0), (1, 0), (1, 1), (0, 1)), ((0, 0), (1, 0), (1, 1), (0, 1)), True),
    (((0, 0), (1, 0), (1, 1), (0, 1)), ((10, 10), (11, 10), (11, 11), (10, 11)), False),

    (((0, 0), (1, 0), (1, 1), (0, 1)), ((2, 0), (2, 0), (3, 1), (3, 1)), False),
    (((0, 0), (1, 0), (1, 1), (0, 1)), ((0, 2), (1, 2), (1, 3), (0, 3)), False),
    (((0, 0), (1, 0), (1, 1), (0, 1)), ((-2, 0), (-2, 0), (-1, 1), (-1, 1)), False),
    (((0, 0), (1, 0), (1, 1), (0, 1)), ((0, -2), (1, -2), (1, -1), (0, -1)), False),

    (((0, 0), (1, 0), (1, 1), (0, 1)), ((0, 0.5), (1, 0.5), (1, 1.5), (0, 1.5)), True),
])
def test_intersect(a, b, expected):
    a = [Coordinates(*c) for c in a]
    b = [Coordinates(*c) for c in b]

    assert intersect(a, b) == expected
