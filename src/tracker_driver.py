#!/usr/bin/env python

from tweet_tracker import *

import signal
import atexit
import pickle
import ConfigParser
import sys
import optparse

import multiprocessing

def main():
    # parse command line options
    usage = 'usage: %prog -e emotion_dumpfile -m market_dumpfile'
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-e', '--emotion-dumpfile', action='store', type='string', dest='emotion_dump_file', default=None, help='read dumped frequencies from file')
    parser.add_option('-m', '--market-dumpfile', action='store', type='string', dest='market_dump_file', default=None, help='read dumped frequencies from file')

    (options, args) = parser.parse_args()

    db_lock = multiprocessing.Lock()

    emotion_process = spawn('emotion', options.emotion_dump_file, db_lock)
    market_process = spawn('market', options.market_dump_file, db_lock)

    emotion_process.join()
    market_process.join()
    



def spawn(class, dump_file, db_lock):
    # register callbacks for exit and interrupt
    atexit.register(email_alert)
    signal.signal(signal.SIGINT, sigint_handler) # respond to SIGINT by dumping

    # get config stuff
    CONFIG_FILE = '../config/%s_tracker.cfg' % class
    config = ConfigParser.ConfigParser()
    with open(CONFIG_FILE) as configfile:
        config.readfp(configfile)
        mysql_user = config.get('mysql', 'user')
        mysql_password = config.get('mysql', 'password')
        home_dir = config.get('dirs', 'home')
        negatives_file = home_dir + '/' + config.get('files', 'negatives')
        log_file = home_dir + '/' + config.get('files', 'log')
        dump_file = home_dir + '/' + config.get('files', 'dump')
        twitter_user = config.get('twitter', 'user')
        twitter_password = config.get('twitter', 'password')

    with open(negatives_file) as negativesfile:
        negatives = negativesfile.read()

    # read dump if specified in command line args
    if dump_file:
        with open(dump_file) as dumpfile:
            dump = pickle.load(dumpfile)
    else:
        dump = ({},0)
    
    # create all the pieces
    logger          = Logger(log_file)
    db              = Database(db_lock, mysql_user, mysql_password)
    if class == 'emotion':
        tracker = EmotionTracker(db, negatives, logger, ['word', 'emoticon'], dump_file, dump)
    elif class == 'market':
        tracker = MarketTracker(db, negatives, logger, ['market'], dump_file, dump)
    else:
        raise ArgumentError('unknown class %s' % class)
    stream  = Stream(twitter_user, twitter_password, tracker.terms, logger)
    crawler = Crawler(stream, [tracker.save_tweet, tracker.handle_tweet])

    # run it
    process = multiprocess.Process(target = crawler.crawl())
    process.start()

    return process
    

if __name__ == "__main__":
    main()
