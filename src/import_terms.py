#!/usr/bin/env python

import MySQLdb

TYPE = 'ticker'
FILE = '../terms/ticker.txt'


def import_terms(terms):
    mysql = MySQLdb.connect(user='tracker', passwd='poodlepaws', db='tweets')
    mysql_cursor = mysql.cursor()

    for term in terms:
        is_negative = term[0] == '-'

        is_parent =  not is_negative and term[0] != '+'

        
        term = term if is_parent else term[1:]

        if is_parent:
            try:
                mysql_cursor.execute("""INSERT INTO terms
                                        (term, parent_id, is_negative, type)
                                        VALUES(%s, %s, %s, %s)"""
                                     , (term, None, is_negative, TYPE))

                mysql_cursor.execute("SELECT LAST_INSERT_ID()")
                last_parent_id = mysql_cursor.fetchone()[0]
            except MySQLdb.IntegrityError:
                mysql_cursor.execute("SELECT id FROM terms WHERE term=%s", term)
                last_parent_id = mysql_cursor.fetchone()[0]

        else:
            try:
                mysql_cursor.execute("""INSERT INTO terms
                                        (term, parent_id, is_negative, type)
                                        VALUES(%s, %s, %s, %s)"""
                                     , (term, last_parent_id, is_negative, TYPE))
            except MySQLdb.IntegrityError:
                pass



def main():
    with open(FILE) as termsfile:
        import_terms(termsfile.read().split('\n'))

if __name__ == '__main__':
    main()
