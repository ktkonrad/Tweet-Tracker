#!/usr/bin/env python

import MySQLdb

TYPE = 'market'
FILE = '../terms/market.txt'


def import_terms(terms):
    mysql = MySQLdb.connect(user='tracker', passwd='poodlepaws', db='tweets')
    mysql_cursor = mysql.cursor()

    for term in terms:
        is_negative = term[0] == '-'

        if not is_negative and term[0] != '+':
            last_parent_id = None
        
        term = term[1:] if last_parent_id else term

        mysql_cursor.execute("""INSERT INTO terms
                                (term, parent_id, is_negative, type)
                                VALUES(%s, %s, %s, %s)"""
                             , (term, last_parent_id, is_negative, TYPE))

        mysql_cursor.execute("SELECT LAST_INSERT_ID()")
        last_parent_id = mysql_cursor.fetchone()[0]

def main():
    with open(FILE) as termsfile:
        import_terms(termsfile.read().split('\n'))

if __name__ == '__main__':
    main()
