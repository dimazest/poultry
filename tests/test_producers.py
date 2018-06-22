from poultry import readline_dir


def test_readline_dir(tweet_collection_dir):
    """Read a tweet collection directory.

    :param str input_dir: the patht to the directory

    :return: an iterable of Tweet objects

    """
    tweets = list(readline_dir(tweet_collection_dir))

    t1, t2, t3 = tweets

    assert t1.id == 190800262909276162
    assert t2.id == 195415832510201856
    assert t3.id == 201239221502099456
