#!/usr/bin/env python

import unittest
import tweet_tracker
import datetime


CONFIG_FILE = 'test_tweet_tracker.cfg'

class TestTweetTracker(unittest.TestCase):
    def setUp(self):
        self.tracker = tweet_tracker.Tracker(CONFIG_FILE)

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

    def test_increment_frequency(self):
        term_id = 1

        # increment positive for new term
        self.tracker.increment_frequency(term_id, 0)
        self.assertEqual(self.tracker.frequencies[term_id], [1,0])
        self.assertEqual(self.tracker.frequencies.keys(), [term_id])

        # increment positive for existing term
        self.tracker.increment_frequency(term_id, 0)
        self.assertEqual(self.tracker.frequencies[term_id], [2,0])
        self.assertEqual(self.tracker.frequencies.keys(), [term_id])

        term_id = 2
        # increment negative for new term
        self.tracker.increment_frequency(term_id, 1)
        self.assertEqual(self.tracker.frequencies[term_id], [0,1])

        # increment positive for existing term
        self.tracker.increment_frequency(term_id, 1)
        self.assertEqual(self.tracker.frequencies[term_id], [0,2])

    def test_increment_frequencies(self):
        terms = ['hopeless', 'calm']
        term_ids, term_is_negatives = zip(*map(self.tracker.get_term_id_and_is_negative, terms))
        text = "I'm not %s. I'm just %s" % tuple(terms)
        self.tracker.increment_frequencies(text)
        self.assertEqual(self.tracker.frequencies[term_ids[0]], [1, 0]) # double negative should be positive
        self.assertEqual(self.tracker.frequencies[term_ids[1]], [1, 0])

    def test_round_to_next_open(self):
        dt1 = datetime.datetime(2012,1,1,14,0,0)
        dt2 = datetime.datetime(2012,1,1,15,0,0)
        self.assertEqual('2012-01-01', tweet_tracker.round_to_next_open(dt1))
        self.assertEqual('2012-01-02', tweet_tracker.round_to_next_open(dt2))

    def test_start_day(self):
        date_str = '2012-01-01'
        first_tweet_id = 123456L
        self.tracker.start_day(date_str, first_tweet_id)
        self.tracker.mysql_cursor.execute("""SELECT `first_tweet_id`
                                                            FROM `daily_data`
                                                            WHERE `date` = %s""",
                                                         date_str)
        db_first_tweet_id = self.tracker.mysql_cursor.fetchone()[0]
        self.tracker.mysql_cursor.execute("""DELETE FROM `daily_data`
                                        WHERE `date` = %s""",
                                     date_str) # cleanup
        self.assertEqual(first_tweet_id, db_first_tweet_id)

    def test_end_day(self):
        date_str = '2012-01-01'
        tweet_count = 42
        self.tracker.start_day(date_str, 0L)
        self.tracker.tweet_count = tweet_count
        self.tracker.end_day(date_str)
        self.tracker.mysql_cursor.execute("""SELECT `tweets_pulled`
                                             FROM `daily_data`
                                             WHERE `date` =  %s""",
                                          date_str)
        db_tweet_count = self.tracker.mysql_cursor.fetchone()[0]
        self.tracker.mysql_cursor.execute("""DELETE FROM `daily_data`
                                        WHERE `date` = %s""",
                                     date_str) # cleanup
        self.assertEqual(tweet_count, db_tweet_count)


    def test_end_day_without_start(self):
        date_str = '2012-01-01'
        tweet_count = 42
        self.tracker.tweet_count = tweet_count
        self.tracker.end_day(date_str)
        self.tracker.mysql_cursor.execute("""SELECT `tweets_pulled`
                                             FROM `daily_data`
                                             WHERE `date` =  %s""",
                                          date_str)
        db_tweet_count = self.tracker.mysql_cursor.fetchone()[0]
        self.tracker.mysql_cursor.execute("""DELETE FROM `daily_data`
                                        WHERE `date` = %s""",
                                     date_str) # cleanup
        self.assertEqual(tweet_count, db_tweet_count)

    def test_write_frequencies(self):
        date_str = '2012-01-01'
        terms = ['hopeless', 'calm']
        term_ids, term_is_negatives = zip(*map(self.tracker.get_term_id_and_is_negative, terms))
        frequencies = [[2, 4], [3, 0]]
        for term_id, frequency in zip(term_ids, frequencies):
            self.tracker.frequencies[term_id] = frequency
        
        self.tracker.write_frequencies(date_str)
        
        for term_id, frequency in zip(term_ids, frequencies):
            self.tracker.mysql_cursor.execute("""SELECT `positive`, `negative`
                                                 FROM `frequencies`
                                                 WHERE `term_id` = %s
                                                   AND `date` = %s""",
                                              (term_id, date_str))
            db_frequency = list(self.tracker.mysql_cursor.fetchone())
            self.tracker.mysql_cursor.execute("""DELETE FROM `frequencies`
                                                 WHERE `term_id` = %s
                                                   AND `date` = %s""",
                                              (term_id, date_str)) # cleanup
            self.assertEqual(frequency, db_frequency)

if __name__ == '__main__':
    unittest.main()
    
