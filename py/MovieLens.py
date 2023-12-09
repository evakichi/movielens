import Commonpackage
import pandas as pd 
import numpy as np
import matplotlib.pyplot as pl

if __name__ == '__main__':
    prog_name,movie_file,rating_file,tag_file,str_threads = Commonpackage.get_args('\"movie file\"','\"rating file\"','\"tag file\"','\"num of threads\"')
    threads = int(str_threads)

    movie = pd.read_csv(movie_file)
    rating = pd.read_csv(rating_file)
    tag = pd.read_csv(tag_file)

    print (movie)
    print (rating)
    print (tag)

    for idx,v in movie.iterrows():
#        print (idx)
        v['genres']=[g for g in v['genres'].split('|') ]
#        print (v['genres'])

#    mgrouped = mf.groupby('title').size().sort_values(ascending=False)
#    print(mgrouped)

#    ratnig_rank = rating.groupby('rating').agg({'rating':'size'})
#    print (ratig_rank)

#    user_rank = rating.groupby('userId',as_index=False).agg('size').sort_values('size',ascending=False).head(1000)
#    print (user_rank)

    movielens = rating.merge(movie,on='movieId').sort_values(['userId','movieId'])
    print (movielens)

#    rating_rank.plot(kind='bar')

#    pl.savefig("/home/evakichi/Pictures/test.png")