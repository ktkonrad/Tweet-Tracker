#!/usr/bin/env python

import datetime
import sys
import pickle
import re

import pymongo
import MySQLdb
import tweetstream
import logging
import smtplib

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
        """log an error message to a logfile"""
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
    def __init__(self, lock, mysql_user, mysql_password, mysql_host='localhost', mysql_db='tweets', mongo_host='localhost', mongo_db='tweets', mongo_port=27017,):
        self.lock = lock

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
        self.lock.acquire()
        self.mysql_cursor.execute(query, params)
        self.lock.release()

    def mysql_fetchone(self, query, params=None):
        self.mysql_cursor.execute(query, params)
        return self.mysql_cursor.fetchone()

    def mysql_fetchall(self, query, params=None):
        self.mysql_cursor.execute(query, params)
        return self.mysql_cursor.fetchall()
        
    def get_terms(self, term_types=[]):
        return [row[0] for row in self.mysql_fetchall("SELECT `term` FROM `terms`" + ("WHERE `type` in ('%s')" % ("','".join(term_types)) if term_types else ""))]

class Crawler:
    """Stream tweets from twitter and execute callbacks"""
    def __init__(self, stream, observers):
        self.stream = stream
        self.observers = observers

    def crawl(self):
        for tweet in self.stream.tweets():
            for observer in self.observers: # TODO: have a thread for each
                try:
                    observer(tweet)
                except: # catch any unhandled exceptions in observer
                    email_alert()
                    raise e

class Tracker:
    def __init__(self, db, negatives, logger, term_types, dump_file, (frequencies, tweet_count)=({},0)):
        self.db = db
        self.negatives = negatives
        self.terms = self.db.get_terms(term_types)
        self.logger = logger
        self.dump_file = dump_file

        # don't access directly. use get_term
        # term : (term_id, is_negative, type)
        self._term_info = dict([(term, (None, None, None)) for term in self.terms])

        # initialize other stuff
        self.last_date = None
        self.frequencies = frequencies
        self.tweet_count = tweet_count

    def get_term(self, term):
        """return id (or None if term is not a term), term.is_negative, term.type
           does a mysql query and memoizes in self._term_info
        """
        term = term[1:] if term[0] == '#' else term
        try:
            (term_id, is_negative, is_word) = self._term_info[term]
            if not term_id:
                row = self.db.mysql_fetchone("""SELECT `id`, `is_negative`, `type` FROM `terms` where `term` = %s""", (term,))
                term_id = row[0]
                is_negative = bool(row[1])
                is_word = row[2]
                self._term_info[term] = (term_id, is_negative, is_word)
            return (term_id, is_negative, is_word)
        except KeyError:
            return (None, None, None)


    def strip_punctuation(self, word):
        """strip trailing punctuation from a word or hashtag"""
        m = re.match("(#?[a-zA-Z\-']+)[\.,!\?;:]", word)
        return m.groups()[0] if m else word

    def is_negative(self, word):
        """return whether a word is negative
        negative words are no, not, cannot, .*n't
        """
        return word in self.negatives

    def is_hashtag(self, word):
        """return whether a word is a hashtag"""
        return bool(re.match('#[a-zA-Z]', word))

    def increment_frequency(self, term_id, component):
        """increment frequency for a single term"""
        if component == 'positive':
            try:
                self.frequencies[term_id][0] += 1
            except KeyError:
                self.frequencies[term_id] = [1, 0, 0]
        elif component == 'negative':
            try:
                self.frequencies[term_id][1] += 1
            except KeyError:
                self.frequencies[term_id] = [0, 1, 0]
        elif component == 'hashtag':
            try:
                self.frequencies[term_id][2] += 1
            except KeyError:
                self.frequencies[term_id] = [0, 0, 1]
        else:
            raise ArgumentError('unknown component %s' % component)

    def dump(self):
        with open(self.dump_file, 'w') as pickle_file:
            pickle.dump((self.frequencies, self.tweet_count), pickle_file)

    def save_tweet(self, tweet):
        self.db.mongo_insert(tweet) # TODO handle exceptions here
            
    def handle_tweet(self, tweet):
        try:
            text = tweet['text']
        except KeyError:
            self.logger.log('No text', tweet)
            return
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
        try:
            self.write_frequencies(date_str)
            self.tweet_count = 0
            self.frequencies = dict()
        except Exception as e:
            self.logger.log(e)
            self.dump()


    def write_frequencies(self, date_str):
        """
        write all the frequencies to sql
        """
        for (term_id, [positive, negative, hashtag]) in self.frequencies.iteritems():
            self.db.mysql_execute("""INSERT INTO `frequencies`
                                     (`term_id`, `date`, `positive`, `negative`, `hashtag`)
                                     VALUES(%s, %s, %s, %s, %s)""",
                                  (term_id, date_str, positive, negative, hashtag))


class EmotionTracker(Tracker):
    """Tracker subclass for tracking emotional words and emoticons"""
    def increment_frequencies(self, text):
        """increment frequencies for all words in text"""
        NEGATIVE_SCOPE = 2 # negative word applies to next 2 words

        last_negative = NEGATIVE_SCOPE + 1
        # TODO: trie matching
        for word in map(self.strip_punctuation, text.lower().split()):
            # check for negative word
            if self.is_negative(word):
                last_negative = 0
            else:
                last_negative += 1

            # get term info
            (term_id, is_negative, term_type) = self.get_term(word)
                
            # increment frequency
            if term_id:
                if self.is_hashtag(word):
                    component = 'hashtag'
                elif is_negative ^ (term_type == 'word' and (last_negative <= NEGATIVE_SCOPE)):
                    component = 'negative'
                else:
                    component = 'positive'

                self.increment_frequency(term_id, component)


class MarketTracker(Tracker):
    """
    Tracker subclass for tracking market terms
    works with n-grams for n=1,2,3
    """

    def grouped_ngrams(self, words, n_max):
        """
        split text into n-grams for n = 1 to n_max
        yields groups as lists
        """
        wordcount = len(words)
        for i in xrange(wordcount):
            yield [' '.join(words[i:j]) for j in xrange(i+1, min(wordcount, i+n_max)+1)]

    def last_truthy_index(self, arr):
        """
        return the index of the last truthy element in arr
        """
        try:
            return [i for (i,x) in enumerate(arr) if x][-1]
        except IndexError:
            return None

    def increment_frequencies(self, text):
        """
        break text into n-grams and increment frequencies
        only use longest n-gram if contained n-grams match
        """
        N = 3 # check for n-grams up to N

        words = map(self.strip_punctuation, text.lower().split())

        last_match_prev = 0
        for gram_group in self.grouped_ngrams(words, N):
            # get term info
            term_ids = [i for i,_,_ in map(self.get_term, gram_group)]
            
            last_match = self.last_truthy_index(term_ids) # can be None
            
            if last_match >= last_match_prev : # all numbers are greater than None
                component = 'hashtag' if self.is_hashtag(gram_group[last_match]) else 'positive'
                self.increment_frequency(term_ids[last_match], component)

            last_match_prev = last_match or 0
            

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
