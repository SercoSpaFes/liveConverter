#!/bin/python
#
#
# will send notification
#
# Lavaux Gilles 2018-01
#


import argparse
import json
import os, sys, time
import traceback

import requests


#
# build graphite event message
#

def buildGraphiteEventMessage(what='default', tags=[], data='test data'):
    aDict ={}
    aDict['what'] = what
    aDict['tags'] = tags
    aDict['when'] = time.time()
    aDict['data'] = data
    return aDict


#
#
#
def sendGraphiteEvent(url, what, tags, data):
    try:
        client = TheClient('graphite', url)
        aDict = buildGraphiteEventMessage(what, tags, data)
        print " will send graphite event: %s" % aDict
        client.sendJasonNotification(aDict)
    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        traceback.print_exc(file=sys.stdout)


#
#  http client
#
class TheClient():
    webHookUrl=None
    senderUser=None
    name=None
    o=None

    #
    #
    #
    def __init__(self, n, u):
        if type(u)==type(""):
            self.setWebHookUrl(u)
        else:
            self.o = u
        self.name = n

    #
    #
    #
    def setWebHookUrl(self, v):
        self.webHookUrl = v

    #
    # payload is a {}
    #
    def sendJasonNotification(self, payload):
        res = requests.post(self.webHookUrl, data=json.dumps(payload))

    #
    #
    #
    def sendTextNotification(self, text, channel, aUser=None):
        payload={}
        #
        if channel is not None:
            if channel[0] not in ['#', '@']:
                channel = '#' + channel
            payload['channel'] = channel

        #
        if aUser is not None:
            payload['username'] = aUser

        #
        payload['text']=text
        #
        try:
            res=None
            res = requests.post(self.webHookUrl, data=json.dumps(payload))
            print " res is:%s; type:%s" % (res, type(res))
            print " res encoding:%s" % res.encoding
            print " res content:%s" % res.content
        except Exception as e:
            sys.stderr.write('An error occurred when trying to deliver the message:\n  {0}'.format(e.message))
            return 2

        if not res.ok:
            sys.stderr.write('Could not deliver the message. Slack says:\n  {0}'.format(res.text))
            return 1
        else:
            return 0

    #
    #
    #
    def toString(self):
        return 'TBD'


#
#
#
def main(args):
    print "Starting"
    try:
        #mesg='this is a test'
        mesg = 'conversion Irs-1C-1D completed'
        channel='general'
        name='test-client'
        if len(args) == 1:
            print "syntax: http_notification_client mesg channel clientName"
            sys.exit(1)

        if len(args) > 1:
            mesg = args[1]
        if len(args) >2:
            channel = args[2]
        if len(args) >3:
            name = args[3]
        print " send notification to 'slack like' service\n  using name:%s; channel:%s; mesg=%s\n or json payload" % (name, channel, mesg)

        #client = TheClient(name, "http:/localhost:8083/events/")
        #client = TheClient('toto', 'http://172.17.63.32:8083/events')
        #client.sendTextNotification(mesg, channel, name)
        sendGraphiteEvent('http://172.17.63.32:8083/events', 'New input', ['liveConversion'], 'a new file: pipo')

    except:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)
    print " done"


if __name__ == '__main__':
    main(sys.argv)
