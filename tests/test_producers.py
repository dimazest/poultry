from poultry import readline_dir


def test_readline_dir(tweet_collection_dir):
    """Read a tweet collection directory.

    :param str input_dir: the patht to the directory

    :return: an iterable of Tweet objects

    """
    tweets = list(readline_dir(tweet_collection_dir))

    t1, t2 = tweets

    assert t2.id == 535432030939791360

    assert t1.text == (
        "i've just came across pydata's Blaze "
        "http://t.co/iMMQAqZzid and it looks very promising"
    )

    assert t1.urls == set(['http://t.co/iMMQAqZzid'])

    assert t1.user_id == 10868922
    assert t2.user_id == 10868922
