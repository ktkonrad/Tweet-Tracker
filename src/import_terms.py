#!/usr/bin/env python

import MySQLdb

def import_terms(terms):
    mysql = MySQLdb.connect(user='tracker', passwd='poodlepaws', db='tweets')
    mysql_cursor = mysql.cursor()
    for term in terms:
        if term[0] == '+':
            mysql_cursor.execute("""INSERT INTO terms
                                    (term, parent_id, is_negative)
                                    VALUES(%s, %s, %s)"""
                                 , (term[1:], last_parent_id, 0))

        elif term[0] == '-':
            mysql_cursor.execute("""INSERT INTO terms
                                    (term, parent_id, is_negative)
                                    VALUES(%s, %s, %s)"""
                                 , (term[1:], last_parent_id, 1))
        else:
            mysql_cursor.execute("""INSERT INTO terms
                                    (term, parent_id)
                                    VALUES(%s, NULL)"""
                                 , (term,))
            mysql_cursor.execute("SELECT LAST_INSERT_ID()")
            last_parent_id = mysql_cursor.fetchone()[0]

def main():
    with open('terms.txt') as termsfile:
        import_terms(termsfile.read().split())

if __name__ == '__main__':
    main()
