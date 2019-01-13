# import dask bag
import dask.bag as db

import time

start = time.perf_counter() 

votes_bag = db.read_text("Votes.tar.xz",compression="xz")

temp = votes_bag.filter(lambda x: x.find("row") >= 0).count().compute()

end = time.perf_counter() 

#

print(temp)
print(end-start,"s")

## 161 mln votes

## no filter 184 s ~ 3 min
## filter 230 s ~ 4 min




