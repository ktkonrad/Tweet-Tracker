import parser, re, math, copy

MAX_HOLE_SIZE = 4
    
def linear_fill(series, dates, curr_hole_size, curr_hole_start, not_for_exp=True):
    if curr_hole_start == 0 or (not_for_exp and curr_hole_size > MAX_HOLE_SIZE):
        return

    v1 = series[dates[curr_hole_start-1]][0]
    v2 = series[dates[curr_hole_start+curr_hole_size]][0]
    drift = (v2-v1) / (curr_hole_size+2.0)
    
    for di in xrange(curr_hole_start, curr_hole_start+curr_hole_size):
        i = dates[di]
        series[i] = v1+(di+1-curr_hole_start)*drift, True

def exp_fill(series, dates, curr_hole_size, curr_hole_start):
    '''
    f(x) = a*e^x+c
    cant really solve this, dont know series too well and the math is hairy man
    so assuming there is an exponenital trend, log everything,
    linear trend it and exponent it again
    '''
    if curr_hole_start == 0:
        return
    
    for d in dates:
        if series[d][0] != 0:
            series[d] = math.log(series[d][0]), series[d][1]
    
    linear_fill(series, dates, curr_hole_size, curr_hole_start, not_for_exp=False)
    
    for d in dates:
        series[d] = math.exp(series[d][0]), series[d][1]

def fill_holes(series, dates, filler):
    curr_hole_size = 0
    in_hole = False
    hole_start = None
    
    for d in dates:
        if d not in series:
            continue
        if type(series[d]) == type((1,2)):
            break
        series[d] = (series[d], False)   #val, filled

    for i in xrange(len(dates)):
        d = dates[i]
        if d not in series:
            continue
        
        if not in_hole and math.isnan(series[d][0]):
            in_hole = True
            hole_start = i
            curr_hole_size = 1
            continue

        elif in_hole:
            if math.isnan(series[d][0]):
                curr_hole_size += 1
            else:
                filler(series, dates,  curr_hole_size, hole_start)
                in_hole = False

    for d in dates:
        if d not in series:
            continue
        if not math.isnan(series[d][0]):
            series[d] = int(round(series[d][0])), series[d][1]

def normalize_date(date):
    if '/' in date:
        month, day, year = date.split('/')
        if len(day) == 1:
            day = '0'+day
        if len(month) == 1:
            month = '0'+month
        return '20'+year+'-'+month+'-'+day
    else:
        return date
        

def parse_tab_sheet(path, res=None, keys=[]):
    f = open(path)
    if res == None: res = dict()

    lines = re.split('[\r\n]', f.read())
    f.close()

    thiskeys = lines[0].lower().split('\t')[1:]
    allkeys = list(set(thiskeys + keys))
    dates = []

    for line in lines[1:]:
        items = line.split('\t')
        date = items[0]
        
        if date == '':
            continue

        date = normalize_date(date)

        vals = items[1:]
        dates.append(date)
        
        for k, val in zip(thiskeys, vals):
            if k == '':
                continue
            if k == 'Terrifying' and date == '2/22/12':
                print val
            if k not in res:
                res[k] = dict()
            if val == '' or val == '#NA':
                if date not in res[k]:
                    res[k][date] = float('nan')
            else:
                res[k][date] = int(val)
    return res, allkeys, dates

            
def parse_reorder_wills(path='wills_viral.txt'):
    res, keys, dates = parse_tab_sheet(path)
    
    last_date = dates[len(dates)-1]
    bad = []
    good = []
    for k in keys:
        if k == '':
            continue
        if math.isnan(res[k][last_date]):
            bad.append(k)
        else:
            good.append(k)
    return good, bad

def dump_cleaned(res, keys, dates, out):
    w = open(out, 'w')
    
    keys.sort()
    w.write('\t')
    for k in keys:
        if k == '':
            continue
        w.write(k+'\t\t')
    w.write('\n')

    for date in dates:
        w.write(date+'\t')
        for k in keys:
            if k == '':
                continue
            if date not in res[k]:
                w.write('#NA\t\t')
            else:
                if math.isnan(res[k][date][0]):
                    w.write('#NA\t\t')
                else:
                    w.write(str(res[k][date][0])+'\t')
                    if res[k][date][1]:
                        w.write(str(res[k][date][1])+'\t')
                    else:
                        w.write('\t')
                        
        w.write('\n')
    w.close()

def combine_sheets(paths):
    res = None
    keys = []
    for path in paths:
        res, keys, dates = parse_tab_sheet(path, res, keys)

    if '' in keys:
        keys.remove('')
    return res, keys, dates

if __name__ == '__main__':
    res, keys, dates = parser.parse_viral('data/viral_clean/')
    parser.dump_viral(res, keys, dates, 'first_batch_viral.txt')

    res, keys, dates = combine_sheets(['wills_uncleaned_terms.csv', 'first_batch_viral.txt', 'main_filled.csv'])
    
    parser.dump_viral(res, keys, dates, 'viral_raw.txt')

    #dump_cleaned(res, keys, dates, 'non_cleaned.txt')
    lin = copy.deepcopy(res)
    exp = copy.deepcopy(res)
    for k in keys:
        if k == '':
            continue
        fill_holes(lin[k], dates, linear_fill)
        fill_holes(exp[k], dates, exp_fill)
    
    dump_cleaned(lin, keys, dates, 'viral_linear_cleaned.txt')
    dump_cleaned(exp, keys, dates, 'viral_exp_cleaned.txt')

    res, keys, dates = parse_tab_sheet('ticker_freq_raw.csv')
    for k in keys:
        fill_holes(res[k], dates, linear_fill)
    dump_cleaned(res, keys, dates, 'ticker_freq_linear_cleaned.txt')
    
    '''
    res, keys, dates = parse_tab_sheet('wills_viral_filled.csv')
    lin = copy.deepcopy(res)
    exp = copy.deepcopy(res)
    for k in keys:
        fill_holes(lin[k], dates, linear_fill)
        fill_holes(exp[k], dates, exp_fill)
    
    dump_cleaned(lin, keys, dates, 'linear_cleaned.txt')
    dump_cleaned(exp, keys, dates, 'exp_cleaned.txt')

    #

    res, keys, dates = parse_tab_sheet('wills_viral_filled.csv')
    series = res['Quiet']
    
    lin = copy.deepcopy(series)
    fill_holes(lin, dates, linear_fill)
    
    exp = copy.deepcopy(series)
    fill_holes(exp, dates, exp_fill)

    print 'orig \tlin \texp'
    for d in dates[:100]:
        print series[d], '\t', lin[d], '\t', exp[d]
    '''

