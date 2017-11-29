# -*- coding: utf-8 -*-
import json
from pprint import pformat

from requests import Session

from poultry.main import dispatcher
from poultry.stream import StreamProducer, EndOfStreamError

import pytest


@pytest.fixture(autouse=True)
def fake_response(monkeypatch, tweets):
    def mocked_post(self, url, **kwargs):

        class FakeResponse:
            def iter_lines(self):
                return iter(t.encode('utf-8') for t in tweets)

            def raise_for_status(self):
                pass

        return FakeResponse()

    monkeypatch.setattr(Session, 'post', mocked_post)


@pytest.fixture(autouse=True)
def mock_StreamProducer(monkeypatch):
    def mocked(self):
        try:
            self._run()
        except EndOfStreamError:
            pass

    monkeypatch.setattr(StreamProducer, 'run', mocked)


def test_show(capfd, poultry_cfg):
    dispatcher.dispatch(
        args='show -s twitter://sample -v -c {}'.format(poultry_cfg).split(),
        scriptname='poultry',
    )

    out, err = capfd.readouterr()
    assert err.endswith('poultry.stream - WARNING - The POST request is sent.\n')

    assert out == (
        u'dimazest: pinkpop pukkelpop paaspop prilpop pedropicopop all use #pp12 :)\n'
        u'https://twitter.com/#!/dimazest/status/190800262909276162\n'
        u'2012-04-13 13:55:02\n'
        u'\n'
        u'dimazest: #pygrunn here i come!\n'
        u'https://twitter.com/#!/dimazest/status/195415832510201856\n'
        u'2012-04-26 07:35:39\n'
        u'\n'
        u'dimazest: that\'s fun \u201c@gorban: http://t.co/rsjGQjCB\u201d\n'
        u'https://twitter.com/#!/dimazest/status/201239221502099456\n'
        u'2012-05-12 09:15:43\n\n'
    )


def test_select(capfd, tweets, poultry_cfg):
    dispatcher.dispatch(
        args='select -s twitter://sample -v -c {}'.format(poultry_cfg).split(),
        scriptname='poultry',
    )

    out, err = capfd.readouterr()
    assert err.endswith('poultry.stream - WARNING - The POST request is sent.\n')

    expected_result = u'\n'.join(tweets) + u'\n\n'
    assert out == expected_result


def test_pprint(capfd, tweets, poultry_cfg):
    dispatcher.dispatch(
        args='pprint -s twitter://sample -v -c {}'.format(poultry_cfg).split(),
        scriptname='poultry',
    )

    out, err = capfd.readouterr()
    assert err.endswith('poultry.stream - WARNING - The POST request is sent.\n')

    expected_result = u'\n'.join(pformat(json.loads(t)) for t in tweets) + '\n'
    assert out == expected_result


def test_timeline(capfd, tweets, poultry_cfg):
    dispatcher.dispatch(
        args='timeline -s twitter://sample -v -c {}'.format(poultry_cfg).split(),
        scriptname='poultry',
    )

    out, err = capfd.readouterr()
    assert err.endswith('poultry.stream - WARNING - The POST request is sent.\n')

    assert out == (
        u'2012-04-13-13 1\n'
        u'2012-04-26-07 1\n'
        u'2012-05-12-09 1\n'
    )
