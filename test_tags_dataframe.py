import dask.dataframe as dd

df = dd.read_csv("TagsFrame-0.csv.tar.xz",compression="xz",blocksize=None)
print(df.head())
print(df.dtypes)
