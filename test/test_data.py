#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 08:48:04 2020

@author: fabian
"""

import pytest

import powerplantmatching as pm
from powerplantmatching import data

sources = ["OPSD", "ENTSOE", "GEO", "JRC", "GPD", "BEYONDCOAL"]


@pytest.mark.parametrize("source", sources)
def test_data_request_raw(source):
    func = getattr(data, source)
    if source == "OPSD":
        kwargs = {"rawDE": True}
    else:
        kwargs = {"raw": True}
    df = func(update=True, **kwargs)
    assert len(df)


@pytest.mark.parametrize("source", sources)
def test_data_request_processed(source):
    func = getattr(data, source)
    df = func()
    assert len(df)


def test_powerplants():
    pm.powerplants(from_url=True)
