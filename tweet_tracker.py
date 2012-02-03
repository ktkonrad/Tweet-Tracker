#!/usr/bin/env python

import tweetstream
import ConfigParser
import pymongo
import MySQLdb
import logging
import datetime
import atexit
import signal
import optparse
import sys
import pickle
import re

## helpers - move these out at some point
utc_open_time = datetime.time(14,30,0) # 9:30am EST = 2:30pm UTC # TODO: daylight savings time
def round_to_next_open(dt):
    """return dt as YYYY-MM-DD string rounded futureward to 9:30am EST"""
    return str(dt.date() if dt.time() < utc_open_time else dt.date() + datetime.timedelta(days=1))
## end helpers


class tracker:
    def __init__(self, config_file, dump_file=None):

        # get config stuff
        config = ConfigParser.ConfigParser()
        with open(config_file) as configfile:
            config.readfp(configfile)
            self.username = config.get('user', 'username')
            self.password = config.get('user', 'password')
            with open(config.get('files', 'negatives')) as negatives_file:
                self.negatives = negatives_file.read().split()
            logging.basicConfig(filename=config.get('log', 'logfile'), level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s') # initialize logging
            self.dump_file = config.get('files', 'dump')
                
        # connect to mongo
        mongo_connection = pymongo.Connection('localhost', 27017)
        self.mongo = mongo_connection.tweets

        # connect to mysql
        self.mysql = MySQLdb.connect(user='tracker', passwd=self.password, db=config.get('mysql', 'db'))
        self.mysql_cursor = self.mysql.cursor()

        # get terms from mysql
        self.mysql_cursor.execute("SELECT `term` FROM `terms`")
        self.terms = [row[0] for row in self.mysql_cursor.fetchall()]

        # don't access directly. use get_term
        # term : (parent_or_self_id, is_negative)
        self._term_info = dict([(term, (None, None, None)) for term in self.terms])

        # initialize other stuff
        self.last_date = None
        self.tweet_count = 0
        self.frequencies = dict()

    def is_negative(self, word):
        """return whether a word is negative
        negative words are no, not, cannot, .*n't
        """
        return word in self.negatives

    def get_term(self, term):
        """if term has a parent
             return parent_id, term.is_negative, term.is_word
           else
              return id (or None if term is not a term), term.is_negative, term.is_word
           does a mysql query and memoizes in self._term_info
        """
        try:
            (term_id, is_negative, is_word) = self._term_info[term]
            if not term_id:
                self.mysql_cursor.execute("""SELECT `id`, `parent_id`, `is_negative`, `type` FROM `terms` where `term` = %s""", (term,))
                row = self.mysql_cursor.fetchone()
                term_id = row[1] or row[0]
                is_negative = bool(row[2])
                is_word = row[3] == 'word'
                self._term_info[term] = (term_id, is_negative, is_word)
            return (term_id, is_negative, is_word)
        except KeyError:
            return (None, None, None)

    def strip_punctuation(self, word):
        """string trailing punctuation from a word"""
        m = re.match("([a-zA-Z\-']+)[\.,!\?;:]", word)
        return m.groups()[0] if m else word

    def increment_frequencies(self, text):
        """increment frequencies for all words in text"""
        
        import string
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
        while True:
            try:
                with tweetstream.FilterStream(self.username, self.password, track=self.terms) as stream:
                    for tweet in stream:
                        self.handle_tweet(tweet)
            except tweetstream.ConnectionError as e:
                self.log(e, None)

    def handle_tweet(self, tweet):
        self.mongo.tweets.insert(tweet) # TODO handle exceptions here
        try:
            text = tweet['text']
        except KeyError:
            self.log('No text', tweet)
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
            self.mysql_cursor.execute("""INSERT INTO `daily_data`
                                          (`date`, `first_tweet_id`)
                                          VALUES(%s, %s)""",
                                      (date_str, first_tweet_id))
        except MySQLdb.IntegrityError as e:
            self.log(e, None)


    def end_day(self, date_str):
        """
        date_str: DD-MM-YYYY of day to end
        write tweets_pulled to sql
        write all counts to sql
        """
        self.mysql_cursor.execute("""UPDATE `daily_data`
                                       SET `tweets_pulled` = %s
                                       WHERE `date` = %s""",
                                  (self.tweet_count, date_str))
        if self.mysql_cursor.rowcount == 0: #  nothing was updated
            self.log('end_day: no row for date %s' % date_str)
            self.mysql_cursor.execute("""INSERT INTO `daily_data`
                                         (`date`, `tweets_pulled`)
                                         VALUES(%s, %s)""",
                                      (date_str, self.tweet_count))
        self.write_frequencies(date_str)
        self.tweet_count = 0
        self.frequencies = dict()

    def write_frequencies(self, date_str):
        """
        write all the frequencies to sql
        """
        for term_id, [positive, negative] in self.frequencies.iteritems():
            self.mysql_cursor.execute("""INSERT INTO `frequencies`
                                           (`term_id`, `date`, `positive`, `negative`)
                                           VALUES(%s, %s, %s, %s)""",
                                      (term_id, date_str, positive, negative))


    def log(self, message, tweet=None):
        """print an error message to a logfile"""
        logging.error('%s # %s', message, tweet)

# globals
the_tracker = None

def main():
    global the_tracker
    usage = 'usage: %prog -d dumpfile'
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-d', '--dumpfile', action='store', type='string', dest='dump_file', default=None, help='read dumped frequencies from file')
    (options, args) = parser.parse_args()

    atexit.register(email_alert)
    CONFIG_FILE = 'tweet_tracker.cfg'
    the_tracker = pickle.load(options.dumpfile) if options.dumpfile else tracker(CONFIG_FILE)
    signal.signal(signal.SIGINT, sigint_handler) # respond to SIGINT by dumping
    try:
        tracker.crawl()
    except Exception as e: # unhandled exception falls through to here
        print e
        tracker.dump()

def sigint_handler(signum, frame):
    global the_tracker
    print 'handling sigint'
    with open(the_tracker.dump_file, 'w') as pickle_file:
        pickle.dump(the_tracker, pickle_file)
    sys.exit(0)

def email_alert():
    import smtplib
    from email.mime.text import MIMEText

    from_email = 'tracker@tweettracker.dartmouth.edu'
    to_emails = ['kyle.t.konrad@gmail.com']
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
        

