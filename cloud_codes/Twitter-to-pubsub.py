# -*- coding: utf-8 -*-
"""
Created on Wed Jul 24 14:18:03 2019

@author: cgutierrez
"""


import base64
import datetime
import os
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

import utils

# Get your twitter credentials from the environment variables.
# These are set in the 'twitter-stream.json' manifest file.
consumer_key = 'EtMLrd8H11BuIYpCIp56AWKnf'
consumer_secret = 'vmD2ECzg5XRgzmdMyhc2m7nYdo7R0fdN1fhMX1ZI4ZnSfyzsIV'
access_token = '142500263-rhpKzqQAiHpJaXJbmCXWa3xTypWzaMSydNE0Mc83'
access_token_secret = 'vpLmeHdfrCmOSoP5fBkE8krjPYP6XZf5Wvy3rJeSKe2bI'

PUBSUB_TOPIC = 'projects/servinf-geomark-social-prod/topics/tweets'
NUM_RETRIES = 3


def publish(client, pubsub_topic, data_lines):
    """Publish to the given pubsub topic."""
    messages = []
    for line in data_lines:
        pub = base64.urlsafe_b64encode(line)
        messages.append({'data': pub})
    body = {'messages': messages}
    resp = client.projects().topics().publish(
            topic=pubsub_topic, body=body).execute(num_retries=NUM_RETRIES)
    return resp


class StdOutListener(StreamListener):
    """A listener handles tweets that are received from the stream.
    This listener dumps the tweets into a PubSub topic
    """

    count = 0
    twstring = ''
    tweets = []
    batch_size = 50
    total_tweets = 10000000
    client = utils.create_pubsub_client(utils.get_credentials())

    def write_to_pubsub(self, tw):
        publish(self.client, PUBSUB_TOPIC, tw)

    def on_data(self, data):
        """What to do when tweet data is received."""
        self.tweets.append(data)
        if len(self.tweets) >= self.batch_size:
            self.write_to_pubsub(self.tweets)
            self.tweets = []
        self.count += 1
        # if we've grabbed more than total_tweets tweets, exit the script.
        # If this script is being run in the context of a kubernetes
        # replicationController, the pod will be restarted fresh when
        # that happens.
        if self.count > self.total_tweets:
            return False
        if (self.count % 1000) == 0:
            print 'count is: %s at %s' % (self.count, datetime.datetime.now())
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':
    print '....'
    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    print 'stream mode is: %s' % os.environ['TWSTREAMMODE']

    stream = Stream(auth, listener)
    # set up the streaming depending upon whether our mode is 'sample', which
    # will sample the twitter public stream. If not 'sample', instead track
    # the given set of keywords.
    # This environment var is set in the 'twitter-stream.yaml' file.
        stream.filter(
                track=['Presidente Duque', 'Iván Duque', 'Duque', 'Presidente Colombia']
                )