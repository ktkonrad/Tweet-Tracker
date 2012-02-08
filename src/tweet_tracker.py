#!/usr/bin/env python

import ConfigParser

import datetime
import atexit
import signal
import optparse
import sys
import pickle
import re

import pymongo
import MySQLdb
import tweetstream
import logging


## helpers - move these out at some point
utc_open_time = datetime.time(14,30,0) # 9:30am EST = 2:30pm UTC # TODO: daylight savings time
def round_to_next_open(dt):
    """return dt as YYYY-MM-DD string rounded futureward to 9:30am EST"""
    return str(dt.date() if dt.time() < utc_open_time else dt.date() + datetime.timedelta(days=1))
## end helpers

class Logger:

    def __init__(self, logfile):
        logging.basicConfig(filename=logfile, level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s') # initialize logging

    def log(self, message, tweet=None):
        """print an error message to a logfile"""
        logging.error('%s # %s', message, tweet)


class Stream:
    """
    a stream of tweets
    basically just a wrapper around tweetstream.FilterStream
    """
    def __init__(self, username, password, terms, logger):
        self.username = username
        self.password = password
        self.terms = terms
        self.logger = logger

    def tweets(self):
        while True:
            try:
                with tweetstream.FilterStream(self.username, self.password, track=self.terms) as stream:
                    for tweet in stream:
                        yield tweet
            except tweetstream.ConnectionError as e:
                self.logger.log(e, None)

class Database:
    """
    mysql and mongo dbs
    Not thread-safe
    """


    def __init__(self, mysql_user, mysql_password, mysql_host='localhost', mysql_db='tweets', mongo_host='localhost', mongo_db='tweets', mongo_port=27017,):
        # connect to mongo
        mongo_connection = pymongo.Connection(mongo_host, mongo_port)
        self.mongo = mongo_connection[mongo_db]

        # connect to mysql
        self.mysql = MySQLdb.connect(user=mysql_user, passwd=mysql_password, db=mysql_db)
        self.mysql_cursor = self.mysql.cursor()
        
    def mongo_insert(self, tweet):
        """insert a tweet into mongo"""
        self.mongo.tweets.insert(tweet)

    def mysql_execute(self, query, params=None):
        self.mysql_cursor.execute(query, params)

    def mysql_fetchone(self, query, params=None):
        self.mysql_cursor.execute(query, params)
        return self.mysql_cursor.fetchone()

    def mysql_fetchall(self, query, params=None):
        self.mysql_cursor.execute(query, params)
        return self.mysql_cursor.fetchall()
        
    def get_terms(self, term_types=[]):
        return [row[0] for row in self.mysql_fetchall("SELECT `term` FROM `terms`" + ("WHERE `type` in ('%s')" % ("','".join(term_types)) if term_types else ""))]



class Tracker:
    def __init__(self, db, negatives, username, password, term_types=[], dump_file='tracker.pckl', log_file = 'tracker.log'):
        self.db = db
        self.negatives = negatives
        self.terms = self.db.get_terms(term_types)
        self.logger = Logger(log_file)
        self.stream = Stream(username, password, self.terms, self.logger)
        self.dump_file = dump_file

        # don't access directly. use get_term
        # term : (term_id, is_negative, is_word)
        self._term_info = dict([(term, (None, None, None)) for term in self.terms])

        # initialize other stuff
        self.last_date = None
        self.tweet_count = 0
        self.frequencies = dict()



    def strip_punctuation(self, word):
        """strip trailing punctuation from a word"""
        m = re.match("([a-zA-Z\-']+)[\.,!\?;:]", word)
        return m.groups()[0] if m else word


    def is_negative(self, word):
        """return whether a word is negative
        negative words are no, not, cannot, .*n't
        """
        return word in self.negatives

    def increment_frequency(self, term_id, is_negative):
        """increment frequency for a single term"""
        if is_negative:
            try:
                self.frequencies[term_id][1] += 1
            except KeyError:
                self.frequencies[term_id] = [0, 1]
        else:
            try:
                self.frequencies[term_id][0] += 1
            except KeyError:
                self.frequencies[term_id] = [1, 0]

    def crawl(self):
        """stream tweets and insert into mongo and mysql"""
        for tweet in self.stream.tweets:
            self.handle_tweet(tweet)
            
    def handle_tweet(self, tweet):
        self.mongo.tweets.insert(tweet) # TODO handle exceptions here
        try:
            text = tweet['text']
        except KeyError:
            self.logger.log('No text', tweet)
        else:
            self.tweet_count += 1
            now_date = round_to_next_open(datetime.datetime.utcnow())
            if now_date != self.last_date:
                if self.last_date:
                    self.end_day(self.last_date)
                self.start_day(now_date, tweet['id'])
            self.increment_frequencies(text)
            self.last_date = now_date

    def start_day(self, date_str, first_tweet_id):
        try:
            self.db.mysql_execute("""INSERT INTO `daily_data`
                                     (`date`, `first_tweet_id`, `tracker_class`)
                                     VALUES(%s, %s, %s)""",
                                  (date_str, first_tweet_id, self.__class__.__name__))
        except MySQLdb.IntegrityError as e:
            self.logger.log(e, None)


    def end_day(self, date_str):
        """
        date_str: DD-MM-YYYY of day to end
        write tweets_pulled to sql
        write all counts to sql
        """
        self.db.mysql_execute("""UPDATE `daily_data`
                           SET `tweets_pulled` = %s,
                               `tracker_class` = %s
                           WHERE `date` = %s""",
                        (self.tweet_count, self.__class__.__name__, date_str))
        if self.db.mysql_cursor.rowcount == 0: #  nothing was updated
            self.logger.log('end_day: no row for date %s' % date_str)
            self.db.mysql_execute("""INSERT INTO `daily_data`
                                     (`date`, `tweets_pulled`, `tracker_class`)
                                     VALUES(%s, %s, %s)""",
                                  (date_str, self.tweet_count, self.__class__.__name__))
        self.write_frequencies(date_str)
        self.tweet_count = 0
        self.frequencies = dict()

    def write_frequencies(self, date_str):
        """
        write all the frequencies to sql
        """
        for (term_id, [positive, negative]) in self.frequencies.iteritems():
            self.db.mysql_execute("""INSERT INTO `frequencies`
                                     (`term_id`, `date`, `positive`, `negative`)
                                     VALUES(%s, %s, %s, %s)""",
                                  (term_id, date_str, positive, negative))


class EmotionTracker(Tracker):
    """Tracker subclass for tracking emotional words and emoticons"""
    def get_term(self, term):
        """return id (or None if term is not a term), term.is_negative, term.is_word
           does a mysql query and memoizes in self._term_info
        """
        try:
            (term_id, is_negative, is_word) = self._term_info[term]
            if not term_id:
                row = self.db.mysql_fetchone("""SELECT `id`, `is_negative`, `type` FROM `terms` where `term` = %s""", (term,))
                term_id = row[0]
                is_negative = bool(row[1])
                is_word = row[2] == 'word'
                self._term_info[term] = (term_id, is_negative, is_word)
            return (term_id, is_negative, is_word)
        except KeyError:
            return (None, None, None)


    def increment_frequencies(self, text):
        """increment frequencies for all words in text"""
        NEGATIVE_SCOPE = 2 # negative word applies to next 2 words

        last_negative = NEGATIVE_SCOPE + 1
        # TODO: trie matching
        for word in map(self.strip_punctuation, text.split()):
            if self.is_negative(word):
                last_negative = 0
            else:
                last_negative += 1
            (term_id, is_negative, is_word) = self.get_term(word)
            if term_id:
                self.increment_frequency(term_id, is_negative ^ (is_word and (last_negative <= NEGATIVE_SCOPE)))



# globals
the_tracker = None

def main():
    global the_tracker
    usage = 'usage: %prog -d dumpfile'
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-d', '--dumpfile', action='store', type='string', dest='dump_file', default=None, help='read dumped frequencies from file')
    (options, args) = parser.parse_args()

    atexit.register(email_alert)
    CONFIG_FILE = '../config/tweet_tracker.cfg'
    if options.dump_file:
        with open(options.dump_file) as dumpfile:
            the_tracker = pickle.load(options.dump_file)
    else:
        the_tracker = tracker(CONFIG_FILE)
    signal.signal(signal.SIGINT, sigint_handler) # respond to SIGINT by dumping
    try:
        the_tracker.crawl()
    except Exception as e: # unhandled exception falls through to here
        print e
        dump()

def sigint_handler(signum, frame):
    global the_tracker
    print 'handling sigint'
    dump()
    sys.exit(0)

def dump():
    with open(the_tracker.dump_file, 'w') as pickle_file:
        pickle.dump(the_tracker, pickle_file)


def email_alert():
    from email.mime.text import MIMEText

    from_email = 'tracker@tweettracker.dartmouth.edu'
    to_emails = ['kyle.t.konrad@gmail.com', 'wills.begor@gmail.com']
    subject = 'alert'
    text = 'tracker failed at %s' % datetime.datetime.now()

    message = MIMEText(text)
    message['Subject'] = subject
    message['From'] = from_email
    message['To'] = ','.join(to_emails)

    s = smtplib.SMTP('localhost')
    s.sendmail(from_email, to_emails, message.as_string())
    s.quit()

if __name__ == "__main__":
    main()
        

#        # get config stuff
#        config = ConfigParser.ConfigParser()
#        with open(config_file) as configfile:
#            config.readfp(configfile)
#            self.home_dir = config.get('dirs', 'home')
#            with open(self.home_dir + '/terms/' + config.get('files', 'negatives')) as negatives_file:
#                self.negatives = negatives_file.read().split()
#            
#            self.dump_file = self.home_dir + '/data/' + config.get('files', 'dump')
#                
#            # connect to mongo
#            mongo_connection = pymongo.Connection(config.get('mongo', 'host'), int(config.get('mongo', 'port')))
#            self.mongo = mongo_connection[config.get('mongo', 'db')]
#
#            # connect to mysql
#            self.mysql = MySQLdb.connect(user='tracker', passwd=self.password, db=config.get('mysql', 'db'))
#            self.mysql_cursor = self.mysql.cursor()
#
#            # get terms from mysql
#            self.mysql_cursor.execute("SELECT `term` FROM `terms`")
#            self.terms = [row[0] for row in self.mysql_cursor.fetchall()]
#
#            self.stream = Stream(config.get('user', 'username'), config.get('user', 'password'), self.terms)
