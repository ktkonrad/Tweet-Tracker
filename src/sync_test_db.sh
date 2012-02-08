#!/bin/bash

mysqldump -utracker -ppoodlepaws tweets --no-data > temp
mysql -utracker -ppoodlepaws -Dtest_tweets < temp
mysqldump -utracker -ppoodlepaws tweets terms > temp
mysql -utracker -ppoodlepaws -Dtest_tweets < temp
rm temp