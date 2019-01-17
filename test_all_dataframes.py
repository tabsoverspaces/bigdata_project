import dask.dataframe as dd

filenames = ["BountyFrame", "DuplicateFrame", "TagsFrame", "UsersFrame", "BadgesFrame", "BadgesAggregatedFrame"]

for filename in filenames:
    print(filename)
    temp_df = dd.read_csv(filename+"-0.csv.tar.xz",compression="xz",blocksize=None)
    print(temp_df.head())
    print(temp_df.dtypes)
    print("-----")