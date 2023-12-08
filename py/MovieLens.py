import Commonpackage
import pandas

if __name__ == '__main__':
    prog_name,movie_file,rating_file,str_threads = Commonpackage.get_args('\"movie file\"','\"rating file\"','\"num of threads\"')
    threads = int(str_threads)

mf = pandas.read_csv(movie_file)
rf = pandas.read_csv(rating_file)

print (rf)

print (mf)
