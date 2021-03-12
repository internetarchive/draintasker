#!/usr/bin/env python3

""" time, date, etc. utils """

__author__ = "siznax 2010"

import os, time, datetime, pprint

def reflect(obj):
    pprint.pprint(obj.__dict__)

def echo_start(name):
    print(name, localtime())

def echo_finish(name=None, seconds=None):
    if name == None:
        print(os.path.basename(__file__))
    else:
        print(name)
    print("Done.", localtime())
    if seconds != None:
        print(str(seconds) + " seconds")

def localtime():
    os.environ['TZ'] = 'US/Pacific'
    time.tzset()
    return time.strftime("%a %b %d %Y %T %Z",time.localtime())

def iso_datetime():
    return time.strftime("%Y-%m-%dT%H:%M:%S%Z",time.localtime())

