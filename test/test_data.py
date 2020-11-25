#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov 25 08:48:04 2020

@author: fabian
"""

import powerplantmatching as pm

def test_OPSD():
    pm.data.OPSD()


def test_powerplants():
    pm.powerplants(from_url=True)