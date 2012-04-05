from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys
import time
import random
import os
import glob
import pickle
import re
import sys

def get_mostrecent_filename(path, pattern='*.csv'):
    m = 0
    mf = None
    for fname in glob.glob(path+pattern):
        atime = os.stat(fname)[8]
        if atime > m:
            m = atime
            mf = fname
    return mf

def find_go_button(b):
    els = b.find_elements_by_tag_name('input')
    for e in els:
        try:
            v = e.get_attribute('value')
            if v == 'Go':
                return e
        except:
            pass

def is_finished_loading(fname):
    try:
        f = open(fname)
        lines = f.readlines()
        f.close()
    except:
        time.sleep(5)
        return False

    for line in lines[10:]:
        if 'RTs' in line:
            continue
        if 'Loading' in line:
            return False
    return True
        

def scrape_pbrowsr(keys, d):
    url = 'http://gr.peoplebrowsr.com'
    
    op = webdriver.ChromeOptions()
    op.add_argument('--incognito')
    b = webdriver.Chrome(chrome_options=op)
    
    b.get(url)
    
    sb = b.find_element_by_id('searchbox_input') 
    csv = b.find_element_by_id('export_csv') 

    for k in keys:
        d[k] = False
        sb.send_keys(k+Keys.RETURN)
        time.sleep(random.randint(0,2))

        try:
            #close pop up the first time
            clb = b.find_element_by_id('close_button')
            clb.click()
            time.sleep(random.randint(0,2))
        except:
            pass

            
        rf = b.find_element_by_id('range_first') 
        rf.find_elements_by_tag_name('option')[0].click()
        time.sleep(1)
        go = find_go_button(b)
        go.click()

        #let it load
        time.sleep(5)

        try:
            clb = b.find_element_by_id('close_avail_on_request')
            clb.click()
            time.sleep(random.randint(0,3))
            d[k] = True
        except:
            pass


        csv.click() #download
        
        #check if we got everything
        fname = get_mostrecent_filename('../../Downloads/')
        while not is_finished_loading(fname):
            if fname != None: os.remove(fname)
            time.sleep(random.randint(5,10))
            csv.click()
            time.sleep(3)
            fname = get_mostrecent_filename('../../Downloads/')

        print k

        #backpsace old query
        time.sleep(random.randint(0,3))
        sb.send_keys(Keys.BACK_SPACE * len(k))

    time.sleep(2)
    b.quit()

def run(keys):
    curr = 0
    d=dict()
    while curr < len(keys):
        ks = keys[curr:curr+5]
        scrape_pbrowsr(ks, d)

        f = open('popup.pkl', 'w')
        pickle.dump(d, f)
        f.close()

        curr+=5
    
def get_browser(usr='', psw=''):
    op = webdriver.ChromeOptions()
    #op.add_argument('--user-data-dir=~/.config/google-chrome/Default')
    op.add_argument('--incognito')
    op.add_argument('--start-maximized')

    #op.binary_location = '/usr/bin/chromium-browser'

    br = webdriver.Chrome(chrome_options=op)
    #br = webdriver.Firefox()
    br.get('http://peoplebrowsr.com')
    #return br
    time.sleep(1)

    #login
    login = br.find_element_by_id('loginPopup')
    login.click()

    while True:
        try:
            time.sleep(3)
    
            uid = br.find_element_by_id('email_input')
            pw  = br.find_element_by_id('pwd_input')
            subm = br.find_element_by_id('checkCredentials')
            break
        except:
            raise
            print 'login error'
            time.sleep(5)
            
    time.sleep(5)
    uid.click()
    uid.send_keys(usr)
    time.sleep(1)

    pw.click()
    pw.send_keys(psw)
    subm.click()
    
    time.sleep(20)
    br.get('http://peoplebrowsr.com/ViralAnalytics')
    time.sleep(1)
    
    build = br.find_element_by_link_text('Build Yours Now')
    build.click()
    time.sleep(2)

    return br

def get_viral_analytics(br, term):
    outer = 0
    retries = 0
    print term
    sel_tw = br.find_element_by_class_name('sel_twitter')
    sel_tw.click()
    
    sb = br.find_element_by_id('searchbox_input') 
    sb.send_keys(100*Keys.BACK_SPACE)
            
    sb.send_keys(term+Keys.RETURN)
            
    bad = False
    els = br.find_elements_by_class_name('mentions')
    att2 = 0
    for mentions in els:
        try:
            arrow = mentions.find_element_by_class_name('chart-options')
            arrow.click()
            break
        except:
            print sys.exc_info()[0], '0'
            time.sleep(0.5)
            continue

        time.sleep(1)
        date_range = arrow.find_element_by_class_name('date_range')
        date_range.click()

        
        #dates
        dates = mentions.find_element_by_class_name('daterange_container')
        fromi = dates.find_element_by_class_name('from_date')
        toi   = dates.find_element_by_class_name('to_date')
        subm  = dates.find_elements_by_tag_name('input')[2]
        
        fromi.send_keys('2008-10-02')
        toi.send_keys('2012-02-22')
        subm.click()
                    
        #go to sleep - up to two min yikes
        time.sleep(20)

        down = 1
        while True:
            arrow.click()
            exp = arrow.find_element_by_class_name('export_csv')
            exp.click()
            time.sleep(5)
            mf = get_mostrecent_filename('../../Downloads/')
            print mf
            f = open(mf)
            n = len(f.readlines())
            f.close()
            
            if n > 100:
                break
            else:
                if down %20 ==0:
                    break
                down+=1
                print mf, n
                os.remove(mf)
                time.sleep(20)
                
                sb.send_keys(Keys.BACK_SPACE * len(term))
                return bad            

def close_browser(br):
    br.quit()
    os.system('killall chrome')
    os.system('killall chromedriver')


def do_for_keys(keys, br):#usr, pw):
    '''apprehension breaks stuff'''
    c = 0
    #br = get_browser(usr, pw)
    for k in keys[c:]:
        a = get_viral_analytics(br, k, usr, pw)
        print a, c
        c+=1

        if c % 50 == 0:
            #close_browser(br)
            time.sleep(1)
            while True:
                try:
                    #br = get_browser(usr, pw)
                    break
                except:
                    time.sleep(1)
                    pass
        
        yield a
    
def load_keywords(path):
    f = open(path)
    keys = map(lambda x: re.sub('[\-\+\r\n]', '', x), f.readlines())
    f.close()
    return keys


if __name__ == '__main__':
    keys = load_keywords('kwords.txt')
    res = list(do_for_keys(keys, 'alexrumenov@yahoo.com', '12345c7890'))
    
