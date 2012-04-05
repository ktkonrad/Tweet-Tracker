import os, re, pickle, math

def parse_csv(path):
    files = os.listdir(path)
    res = dict()
    for f in files:
        keyword = re.search('PeopleBrowsr (.+?) Keyword', f)
        if keyword: 
            keyword = keyword.group(1)
        else: 
            continue

        if keyword in res and '(1)' not in f:
            continue
        res[keyword] = dict()
            
        r = open(path+f)
        lines = r.readlines()
        r.close()
        
        for line in lines:
            if 'Word Frequency Table' in line:
                break

            vals = line.split(',')
            res[keyword][vals[0]] = vals[1:]

        print keyword
    return res

def dump_tabs(res, out, KEY='"Twitter Mentions"'):
    w = open(out, 'w')
    w.write(KEY+'\nKeyword:\t')
    for k in res.keys():
        w.write(k+'\t')
        if '' in res[k] and len(res[k]['']) > 100:
            dates = res[k]['']
    
    w.write('\n')


    c = 0
    for d in dates:
        w.write(d.strip()+'\t')
        for k in res.keys():
            if KEY not in res[k]:
                w.write('#NA\t')
                continue

            curr = res[k][KEY]
            if c < len(curr): w.write(re.sub('[\t\r\n]', '', curr[c]))
            else: w.write('#NA')
            w.write('\t')
        w.write('\n')
        c+=1
    w.close()

def parse_viral(path):
    files = os.listdir(path)
    res = dict()
    keys = []
    dates = []
    first = True
    
    for f in files:
        keyword = re.search('Twitter (.+?) Chart', f)
        if keyword: 
            keyword = keyword.group(1)
        else: 
            continue

        res[keyword] = dict()
            
        r = open(path+f)
        lines = r.readlines()
        r.close()
        
        for line in lines[1:]:
            date, val = line.split(',')
            if val == '\n':
                val = '#NA'
            else:
                val = int(val)
            res[keyword][date] = val
            
            if first:
                dates.append(date)
        
        first = False
        if len(dates) !=  res[keyword].keys():  pass#print 'Not the same dates everyehre!'
        keys.append(keyword)

        #print keyword
    return res, keys, dates

def dump_viral(res, keys, dates, out):
    w = open(out, 'w')

    w.write('\t')
    for k in keys:
        w.write(k+'\t')
    w.write('\n')

    for date in dates:
        w.write(date+'\t')
        for k in keys:
            if date not in res[k] or res[k][date] == '#NA' or math.isnan(res[k][date]):
                w.write('#NA\t')
            else:
                w.write(str(res[k][date]))
                w.write('\t')
        w.write('\n')
    w.close()
        
    
if __name__ == '__main__':
    res, keys, dates = parse_viral('data/viral_clean/')
    dump_viral(res, keys, dates, 'first_batch_viral.txt')
