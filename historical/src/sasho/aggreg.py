import parser, sweeper, re, math

class Aggreg:
    def __init__(self, name, res):
        self.name = name
        self.series = res[name]
        self.positive_keys = []
        self.negative_keys = []

        self.res = res

    def str_name(self, diff):
        if len(self.negative_keys) == 0 and len(self.positive_keys) == 0:
            return self.name

        if diff:
            return '+'+self.name+'\t-'+self.name
        else:
            return self.name
        
    def add_pos(self, key):
        self.positive_keys.append(key)
    
    def add_neg(self, key):
        self.negative_keys.append(key)

    def get_date(self, date, diff):
        '''diff tells you if you need to disntguish between pos and neg children'''
        pscore, nscore = 0, 0

        if date in self.series and not math.isnan(self.series[date]):
            pscore = self.series[date]

        for key in self.positive_keys:
            if date in res[key] and not math.isnan(res[key][date]):
                pscore += res[key][date]

        for key in self.negative_keys:
            if date in res[key] and not math.isnan(res[key][date]):
                nscore += res[key][date]
        
        if pscore == 0: pscore = float('nan')
        if nscore == 0: nscore = float('nan')

        if diff:
            return pscore, nscore
        else:
            return pscore+nscore

    def str_date(self, date, diff):
        if diff:
            pscore, nscore = self.get_date(date, diff)
            if len(self.positive_keys) == 0 and len(self.negative_keys) == 0:
                return str(pscore)

            if math.isnan(pscore): pscore = '#NA'
            if math.isnan(nscore): nscore = '#NA'
            return str(pscore)+'\t'+str(nscore)
        else:
            score = get_date(self, date, diff)
            if math.isnan(score): score = '#NA'
            return str(score)

    @staticmethod
    def get_aggregs(res, kwords_file):
        f = open(kwords_file)
        
        aggregs = []
        curr = None

        for line in f:
            key = line.strip()
            if key[0] == '+':
                curr.add_pos(key[1:])
            elif key[0] == '-':
                curr.add_neg(key[1:])
            else:
                curr = Aggreg(key, res)
                aggregs.append(curr)

        f.close()
        
        return aggregs

    @staticmethod
    def write_aggregs(aggregs, dates, out, diff=True):
        w = open(out, 'w')

        w.write('\t')
        for a in aggregs:
            w.write(a.str_name(diff)+'\t')
        w.write('\n')
        
        for date in dates:
            w.write(date+'\t')
            for a in aggregs:
                w.write(a.str_date(date, diff)+'\t')
            w.write('\n')

        w.close()


if __name__ == '__main__':
    res, keys, dates = sweeper.parse_tab_sheet('viral_linear_cleaned.txt')
    aggregs = Aggreg.get_aggregs(res, 'kwords/terms.txt')
    Aggreg.write_aggregs(aggregs, dates, 'aggregated_emotions.txt')
