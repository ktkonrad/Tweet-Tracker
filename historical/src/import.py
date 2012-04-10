"""import historical data to mysql"""

import MySQLdb
import os
import re
import csv
import sys

POSITIVE_DIR = '../data/positive'
NEGATIVE_DIR = '../data/negative'
HASHTAG_DIR = '../data/hashtag'
TICKER_DIR = '../data/ticker'

MYSQL_USER = 'tracker'
MYSQL_PASSWORD = 'poodlepaws'
MYSQL_DB = 'tweets'

mysql_cursor = None

def get_term_id(term):
    global mysql_cursor
    mysql_cursor.execute("SELECT id FROM terms WHERE term=%s", term)
    try:
        return mysql_cursor.fetchone()[0]
    except TypeError: # if fetchone returns None
        raise ValueError("Couldn't find term: %s" % term)

def import_file(filename, term_type):
    global mysql_cursor

    if term_type not in ['positive', 'negative', 'hashtag']:
        raise ValueError('Invalid term_type: %s' % term_type)

    m = re.search("/#?(\$?[\w '\-&\.]+).txt", filename)
    if not m:
        raise ValueError('failed to parse file name: %s' % filename)
    term = m.groups(1)[0]
    if term_type == 'negative': 
        term = term.split()[-1] # drop the negation word (use -1 instead of 1 for 'not a pessimist.txt'
    try:
        term_id = get_term_id(term)
    except ValueError as e:
        print >> sys.stderr, e
        return

    with open(filename) as f:
        for line in f:
            [date, freq] = line[:-1].split(',') # remove trailing newline
            if freq == '#NA' or freq == '':
                continue
            print 'inserting: %s, %s, %s, %s' % (term_id, term_type, date, freq)
            try:
                mysql_cursor.execute("INSERT INTO frequencies (term_id, date, %s) VALUES(%%s, %%s, %%s)" % term_type, (term_id, date, freq))
            except MySQLdb.IntegrityError:
                mysql_cursor.execute("UPDATE frequencies SET %s = %%s WHERE term_id = %%s AND date = %%s" % term_type, (freq, term_id, date))
            # negatives are also counted as positives so we have to subtract them off
            if term_type == 'negative':
                mysql_cursor.execute("UPDATE frequencies SET positive = positive - %s WHERE term_id = %s AND date = %s", (freq, term_id, date))

def setup():
    global mysql_cursor
    mysql = MySQLdb.connect(user=MYSQL_USER, passwd=MYSQL_PASSWORD, db=MYSQL_DB)
    mysql_cursor = mysql.cursor()

def main():
    setup()
#    for filename in os.listdir(POSITIVE_DIR):
#        import_file('%s/%s' % (POSITIVE_DIR, filename), 'positive')
#    for filename in os.listdir(NEGATIVE_DIR):
#        import_file('%s/%s' % (NEGATIVE_DIR, filename), 'negative')
#    for filename in os.listdir(HASHTAG_DIR):
#        import_file('%s/%s' % (HASHTAG_DIR, filename), 'hashtag')
    for filename in os.listdir(TICKER_DIR):
        import_file('%s/%s' % (TICKER_DIR, filename), 'positive')

if __name__ == '__main__':
    main()
