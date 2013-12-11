from poultry.tweet import Tweet, TweetValueError

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
