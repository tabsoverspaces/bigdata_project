import dask.dataframe as dd
from multiprocessing import Pool

## lets go parallel

def process_df(filename):
    temp_df = dd.read_csv(filename+"-0.csv.tar.xz",compression="xz",blocksize=None)
    print(filename + "\n", temp_df.head(),"\n", temp_df.dtypes, "\n-----\n")
    del temp_df
    return ""

p = Pool()

filenames = ["BountyFrame", "DuplicateFrame", "TagsFrame", "UsersFrame", "BadgesFrame", "BadgesAggregatedFrame"]

p.map(process_df, filenames)

# this kills memory 
#filenames = ["PostsFrame", "AnswersFrame"]
#p.map(process_df, filenames)
  