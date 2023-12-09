import Commonpackage
import pandas as pd 
import numpy as np
import matplotlib.pyplot as pl

if __name__ == '__main__':
    prog_name,movie_file,rating_file,str_threads = Commonpackage.get_args('\"movie file\"','\"rating file\"','\"num of threads\"')
    threads = int(str_threads)

    mf = pd.read_csv(movie_file)
    rf = pd.read_csv(rating_file)

    print (rf)

    print (mf)

    for idx,v in mf.iterrows():
#        print (idx)
        v['genres']=[g for g in v['genres'].split('|') ]
#        print (v['genres'])

#    mgrouped = mf.groupby('title').size().sort_values(ascending=False)
#    print(mgrouped)

    rgroupedd = rf.groupby('rating').agg({'rating':len})
    print (rgroupedd)

#    rgrouped = rf.groupby('rating').size().sort_values(ascending=False)
#    print(rgrouped)

    rgroupedd.plot(kind='bar')

    pl.savefig("/home/evakichi/Pictures/test.png")