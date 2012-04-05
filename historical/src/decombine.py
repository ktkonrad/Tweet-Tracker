import sys
import linecache

def decombine(filename, outdir):
    """split an aggregated file of historical frequencies into individual files (one for each column)"""
    with open(filename) as infile:
        data = [line.split('\t') for line in infile.readlines()]
        columns = zip(*data) # transpose it so we can iterate over columns easily
        dates = columns[0][1:]
        for col in columns[1:]:
            with open('%s/%s.txt' % (outdir, col[0]), 'w') as outfile:
                outfile.write('\n'.join(['%s, %s' % (date, freq) for date, freq in zip(dates, col[1:])]))

def main():
    if len(sys.argv) != 3:
        print 'usage: ./decombine.py file_to_split output_directory'
        exit(-1)
    decombine(sys.argv[1], sys.argv[2])

if __name__ == '__main__':
    main()
