#!/usr/bin/env python

from tweet_tracker import *

import signal
import atexit
import pickle
import ConfigParser
import sys
import optparse

import os
import multiprocessing
import time

def main():
    # parse command line options
    usage = 'usage: %prog -e emotion_dumpfile -m market_dumpfile'
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-e', '--emotion-dumpfile', action='store', type='string', dest='emotion_dump_file', default=None, help='read dumped frequencies from file')
    parser.add_option('-m', '--market-dumpfile', action='store', type='string', dest='market_dump_file', default=None, help='read dumped frequencies from file')

    (options, args) = parser.parse_args()

    db_lock = multiprocessing.Lock()

    market_process = multiprocessing.Process(target = spawn, args = ('market', options.market_dump_file, db_lock))
    # market_process.daemon = True
    market_process.start()
    print "%d: I'm the market process" % market_process.pid

    time.sleep(2)
    
    emotion_process = multiprocessing.Process(target = spawn, args = ('emotion', options.emotion_dump_file, db_lock))
    # emotion_process.daemon = True
    emotion_process.start()
    print "%d: I'm the emotion process" % emotion_process.pid

    print 'waiting...'
    market_process.join()
    emotion_process.join()
    print 'exiting'

def spawn(class_name, dump_file, db_lock):
    # register callbacks for exit and interrupt
    atexit.register(email_alert)

    # get config stuff
    CONFIG_FILE = '../config/%s_tracker.cfg' % class_name
    config = ConfigParser.ConfigParser()
    with open(CONFIG_FILE) as configfile:
        config.readfp(configfile)
        mysql_user = config.get('mysql', 'user')
        mysql_password = config.get('mysql', 'password')
        home_dir = config.get('dirs', 'home')
        tweet_dir = home_dir + '/' + config.get('dirs', 'tweets')
        negatives_file = home_dir + '/' + config.get('files', 'negatives')
        log_file = home_dir + '/' + config.get('files', 'log')
        dump_file = home_dir + '/' + config.get('files', 'dump')
        twitter_user = config.get('twitter', 'user')
        twitter_password = config.get('twitter', 'password')

    with open(negatives_file) as negativesfile:
        negatives = negativesfile.read()

    # read dump if specified in command line args
    try:
        with open(dump_file) as dumpfile:
            dump = pickle.load(dumpfile)
    except IOError, TypeError:
        dump = ({},0)
    
    # create all the pieces
    logger          = Logger(log_file)
    db              = Database(db_lock, mysql_user, mysql_password)
    if class_name == 'emotion':
        tracker = EmotionTracker(db, negatives, logger, ['word', 'emoticon'], dump_file, tweet_dir, dump)
    elif class_name == 'market':
        tracker = MarketTracker(db, negatives, logger, ['market', 'ticker'], dump_file, tweet_dir, dump)
    else:
        raise ArgumentError('unknown class %s' % class_name)
    stream  = Stream(twitter_user, twitter_password, tracker.terms, logger)
    crawler = Crawler(stream, [tracker.handle_tweet])

    # run it
    crawler.crawl(class_name)
    

if __name__ == "__main__":
    main()
