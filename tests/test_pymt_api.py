#!/usr/bin/env python

"""Tests for `pymt_api` package."""

import pytest
from pymt_api import PyMT_API as API


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    # import requests
    # return requests.get('https://github.com/rkahun/pymt_api')


def test_content(response):
    """Sample pytest test function with the pytest fixture as an argument."""
    # from bs4 import BeautifulSoup
    # assert 'GitHub' in BeautifulSoup(response.content).title.string


def test_api():
    """Check connection"""
    assert not API().is_connected
