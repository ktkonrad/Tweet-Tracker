#!/usr/bin/env python

import unittest
import tweet_tracker
import datetime
import ConfigParser
import multiprocessing
import gzip
import os
import time

CONFIG_FILE = '../config/test_tweet_tracker.cfg'

class TestEmotionTracker(unittest.TestCase):
    def setUp(self):
        config = ConfigParser.ConfigParser()
        with open(CONFIG_FILE) as configfile:
            config.readfp(configfile)
            mysql_user = config.get('mysql', 'user')
            mysql_password = config.get('mysql', 'password')
            mysql_db = config.get('mysql', 'db')
            home_dir = config.get('dirs', 'home')
            tweet_dir = home_dir + '/' + config.get('dirs', 'tweets')
            negatives_file = home_dir + '/' + config.get('files', 'negatives')
            with open(negatives_file) as negativesfile:
                negatives = negativesfile.read().split()
            dump_file = home_dir + '/' + config.get('files', 'dump')
            log_file = home_dir + '/' + config.get('files', 'log')

        self.tweet_dir = tweet_dir

        db_lock = multiprocessing.Lock()
        db = tweet_tracker.Database(db_lock, mysql_user, mysql_password, mysql_db=mysql_db)
        logger = tweet_tracker.Logger(log_file)
        self.tracker = tweet_tracker.EmotionTracker(db, negatives, logger, ['word', 'emoticon'], dump_file, tweet_dir)
        
        self.tracker.frequencies = {} # this isn't empty sometimes for some reason. could not reproduce outside of unittest

    def tearDown(self):
        # remove all files in tweets/test
        for f in os.listdir(self.tweet_dir):
            file_path = os.path.join(self.tweet_dir, f)
            try:
                os.remove(file_path)
            except Exception, e:
                print e

    def test_is_negative(self):
        self.assertTrue(self.tracker.is_negative('not'))
        self.assertTrue(self.tracker.is_negative('no'))
        self.assertTrue(self.tracker.is_negative('cannot'))
        self.assertTrue(self.tracker.is_negative("didn't"))
        self.assertTrue(self.tracker.is_negative("can't"))
        self.assertTrue(self.tracker.is_negative("shouldn't"))
        self.assertTrue(self.tracker.is_negative("wouldn't"))
        self.assertTrue(self.tracker.is_negative("didnt"))
        self.assertTrue(self.tracker.is_negative("cant"))
        self.assertTrue(self.tracker.is_negative("shouldnt"))
        self.assertTrue(self.tracker.is_negative("wouldnt"))
        self.assertFalse(self.tracker.is_negative('great'))
        self.assertFalse(self.tracker.is_negative('awful'))
        self.assertFalse(self.tracker.is_negative(':)'))
        self.assertFalse(self.tracker.is_negative(':('))

    def test_increment_frequency_parent_positive(self):
        term_id = self.tracker.db.mysql_fetchone("SELECT `id` FROM `terms` WHERE `parent_id` IS NULL LIMIT 1")[0]
        
        # increment positive for new term
        self.tracker.increment_frequency(term_id, 'positive')
        self.assertEqual(self.tracker.frequencies[term_id], [1,0,0])
        self.assertEqual(self.tracker.frequencies.keys(), [term_id])

        # increment positive for existing term
        self.tracker.increment_frequency(term_id, 'positive')
        self.assertEqual(self.tracker.frequencies[term_id], [2,0,0])
        self.assertEqual(self.tracker.frequencies.keys(), [term_id])

    def test_increment_frequency_parent_negative(self):
        term_id = self.tracker.db.mysql_fetchone("SELECT `id` FROM `terms` WHERE `parent_id` IS NULL LIMIT 1")[0]

        # increment negative for new term
        self.tracker.increment_frequency(term_id, 'negative')
        self.assertEqual(self.tracker.frequencies[term_id], [0,1,0])

        # increment negative for existing term
        self.tracker.increment_frequency(term_id, 'negative')
        self.assertEqual(self.tracker.frequencies[term_id], [0,2,0])

    def test_increment_frequency_parent_hashtag(self):
        term_id = self.tracker.db.mysql_fetchone("SELECT `id` FROM `terms` WHERE `parent_id` IS NULL LIMIT 1")[0]

        # increment negative for new term
        self.tracker.increment_frequency(term_id, 'hashtag')
        self.assertEqual(self.tracker.frequencies[term_id], [0,0,1])

        # increment negative for existing term
        self.tracker.increment_frequency(term_id, 'hashtag')
        self.assertEqual(self.tracker.frequencies[term_id], [0,0,2])

    def test_increment_frequency_child(self):
        (term_id, parent_id) = self.tracker.db.mysql_fetchone("SELECT `id`, `parent_id` FROM `terms` WHERE `parent_id` IS NOT NULL LIMIT 1")
        # increment positive for child term
        self.tracker.increment_frequency(term_id, 'positive')
        self.assertEqual(self.tracker.frequencies[term_id], [1,0,0])
        self.assertNotIn(parent_id, self.tracker.frequencies.keys())

    def test_increment_frequencies(self):
        terms = ['hopeless', 'calm', ':)', '8)', 'worry']
        (term_ids, term_is_negatives, _) = zip(*map(self.tracker.get_term, terms))
        text = "I'm not %s. I'm just %s %s I'm no %s #%s" % tuple(terms)
        self.tracker.increment_frequencies(text)
        self.assertEqual(self.tracker.frequencies[term_ids[0]], [1,0,0]) # double negative should be positive
        self.assertEqual(self.tracker.frequencies[term_ids[1]], [1,0,0])
        self.assertEqual(self.tracker.frequencies[term_ids[2]], [1,0,0])
        self.assertEqual(self.tracker.frequencies[term_ids[3]], [1,0,0]) # negative word shouldn't affect emoticon valence
        self.assertEqual(self.tracker.frequencies[term_ids[4]], [0,0,1]) # hashtag

    def test_round_to_next_open(self):
        dt1 = datetime.datetime(2012,1,1,13,0,0)
        dt2 = datetime.datetime(2012,1,1,15,0,0)
        self.assertEqual('2012-01-01', tweet_tracker.round_to_next_open(dt1))
        self.assertEqual('2012-01-02', tweet_tracker.round_to_next_open(dt2))

    def test_start_day(self):
        date_str = '2012-01-01'
        first_tweet_id = 123456L

        self.tracker.start_day(date_str, first_tweet_id)

        (db_first_tweet_id,) = self.tracker.db.mysql_fetchone("""SELECT `first_tweet_id`
                                                                 FROM `daily_data`
                                                                 WHERE `date` = %s
                                                                   AND `tracker_class` = %s""",
                                                              (date_str, self.tracker.__class__.__name__))
        # cleanup
        self.tracker.db.mysql_execute("""DELETE FROM `daily_data`
                                         WHERE `date` = %s
                                           AND `tracker_class` = %s""",
                                      (date_str, self.tracker.__class__.__name__))

        self.assertEqual(first_tweet_id, db_first_tweet_id)

    def test_end_day(self):
        date_str = '2012-01-01'
        tweet_count = 42

        self.tracker.start_day(date_str, 0L)
        self.tracker.tweet_count = tweet_count
        self.tracker.end_day(date_str)

        self.tracker.db.mysql_cursor.execute("""SELECT `tweets_pulled`
                                                FROM `daily_data`
                                                WHERE `date` = %s
                                                  AND `tracker_class` = %s""",
                                             (date_str, self.tracker.__class__.__name__))

        (db_tweet_count,) = self.tracker.db.mysql_cursor.fetchone()

        # cleanup
        self.tracker.db.mysql_cursor.execute("""DELETE FROM `daily_data`
                                                WHERE `date` = %s
                                                  AND `tracker_class` = %s""",
                                             (date_str, self.tracker.__class__.__name__))
        self.assertEqual(tweet_count, db_tweet_count)


    def test_end_day_without_start(self):
        date_str = '2012-01-01'
        tweet_count = 42
        self.tracker.tweet_count = tweet_count
        self.tracker.end_day(date_str) 
        self.tracker.db.mysql_cursor.execute("""SELECT `tweets_pulled`
                                             FROM `daily_data`
                                             WHERE `date` =  %s
                                               AND `tracker_class` = %s""",
                                             (date_str, self.tracker.__class__.__name__))

        db_tweet_count = self.tracker.db.mysql_cursor.fetchone()[0]
        self.tracker.db.mysql_cursor.execute("""DELETE FROM `daily_data`
                                        WHERE `date` = %s
                                          AND `tracker_class` = %s""",
                                             (date_str, self.tracker.__class__.__name__)) # cleanup
        self.assertEqual(tweet_count, db_tweet_count)

    def test_get_term(self):
        terms = ['hope', 'hopeless', ':)']
        for term in terms:
            row = self.tracker.db.mysql_fetchone("""SELECT `id`, `is_negative`, `type` FROM `terms` where `term` = %s""", (term,))
            term_id = row[0]
            is_negative = bool(row[1])
            term_type = row[2]
            self.assertEqual((term_id, is_negative, term_type), self.tracker.get_term(term))
            self.assertEqual((term_id, is_negative, term_type), self.tracker._term_info[term]) # make sure it gets cached correctly

        

    def test_write_frequencies(self):
        date_str = '2012-01-01'
        terms = ['hopeless', 'calm', ':)']
        (term_ids, term_is_negatives, _) = zip(*map(self.tracker.get_term, terms))
        frequencies = [[2, 4, 0], [3, 1, 2], [2, 0, 1]]
        for term_id, frequency in zip(term_ids, frequencies):
            self.tracker.frequencies[term_id] = frequency
        
        self.tracker.write_frequencies(date_str)
        
        for term_id, frequency in zip(term_ids, frequencies):
            db_frequency = list(self.tracker.db.mysql_fetchone("""SELECT `positive`, `negative`, `hashtag`
                                                                  FROM `frequencies`
                                                                  WHERE `term_id` = %s
                                                                    AND `date` = %s""",
                                                               (term_id, date_str)))
            # cleanup
            self.tracker.db.mysql_execute("""DELETE FROM `frequencies`
                                             WHERE `term_id` = %s
                                               AND `date` = %s""",
                                          (term_id, date_str))
            self.assertEqual(frequency, db_frequency)

    def test_strip_punctuation(self):
        strings          = ['hello', 'hello.', 'hello!?', "didn't", "didn't?", 'mad-cool...', ':)', 'XD', ':!', '#great']
        stripped_strings = ['hello', 'hello',  'hello',   "didn't", "didn't",  'mad-cool',    ':)', 'XD', ':!', '#great']
        for (string, stripped_string) in zip(strings, stripped_strings):
            self.assertEqual(stripped_string, self.tracker.strip_punctuation(string))

    def test_rotate_tweetfile(self):
        tweet = os.urandom(1000000) # 1 MB of random data
        date_str = '2012-01-01'
        self.tracker.save_tweet(tweet)
        self.tracker.rotate_tweetfile(date_str)
        zipfile = '%s/tweets_%s.txt.gz' % (self.tracker.tweet_dir, date_str)
        # wait for compression to finish
        # this is a hack and not safe
        # the file may exist but still be being written to
        # works fine for 1MB file though
        while not os.path.exists(zipfile):
            time.sleep(1)
        with gzip.open(zipfile, 'rb') as zipped:
            read_tweet = zipped.read()
        self.assertEqual(tweet + '\n', read_tweet)

class TestMarketTracker(unittest.TestCase):
    def setUp(self):
        config = ConfigParser.ConfigParser()
        with open(CONFIG_FILE) as configfile:
            config.readfp(configfile)
            mysql_user = config.get('mysql', 'user')
            mysql_password = config.get('mysql', 'password')
            mysql_db = config.get('mysql', 'db')
            home_dir = config.get('dirs', 'home')
            tweet_dir = home_dir + '/' + config.get('dirs', 'tweets')
            negatives_file = home_dir + '/' + config.get('files', 'negatives')
            with open(negatives_file) as negativesfile:
                negatives = negativesfile.read().split()
            dump_file = home_dir + '/' + config.get('files', 'dump')
            log_file = home_dir + '/' + config.get('files', 'log')

        self.tweet_dir = tweet_dir

        db_lock = multiprocessing.Lock()
        db = tweet_tracker.Database(db_lock, mysql_user, mysql_password, mysql_db=mysql_db)
        logger = tweet_tracker.Logger(log_file)
        self.tracker = tweet_tracker.MarketTracker(db, negatives, logger, ['market'], dump_file, tweet_dir)
        
        self.tracker.frequencies = {} # this isn't empty sometimes for some reason. could not reproduce outside of unittest
       

    def tearDown(self):
        # remove all files in tweets/test
        for f in os.listdir(self.tweet_dir):
            file_path = os.path.join(self.tweet_dir, f)
            try:
                os.remove(file_path)
            except Exception, e:
                print e

    def test_grouped_ngrams(self):
        words = ['a','b','c','d','e']
        n_max = 3
        my_groups = [['a', 'a b', 'a b c'], ['b', 'b c', 'b c d'], ['c', 'c d', 'c d e'], ['d', 'd e'], ['e']]
        groups = list(self.tracker.grouped_ngrams(words, n_max))
        self.assertEqual(my_groups, groups)

    def test_last_truthy_index(self):
        arr = [None, None, 23, None, 11, None, None]
        last_truthy_index = 4
        self.assertEqual(last_truthy_index, self.tracker.last_truthy_index(arr))
        
    def test_increment_frequencies(self):
        terms = ['djia', 'stock market crash', 'gold']
        subterms = ['stock market', 'market crash']
        (term_ids, term_is_negatives, _) = zip(*map(self.tracker.get_term, terms))
        (subterm_ids, _, _) = zip(*map(self.tracker.get_term, subterms))
        text = "the %s sucks after the %s. buy #%s" % tuple(terms)
        self.tracker.increment_frequencies(text)
        self.assertEqual(self.tracker.frequencies[term_ids[0]], [1,0,0])
        self.assertEqual(self.tracker.frequencies[term_ids[1]], [1,0,0])
        self.assertEqual(self.tracker.frequencies[term_ids[2]], [0,0,1]) # hashtag
        self.assertNotIn(subterm_ids[0], self.tracker.frequencies.keys()) # subterms shouldn't get incremented
        self.assertNotIn(subterm_ids[1], self.tracker.frequencies.keys()) # subterms shouldn't get incremented

if __name__ == '__main__':
    unittest.main()
    
