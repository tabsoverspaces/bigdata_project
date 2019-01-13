# import dask bag
import dask.bag as db
# import BeatuifulSoup
from bs4 import BeautifulSoup

post_history = db.read_text("PostHistory.tar.xz", compression="xz").str.strip()
post_history.take(1)

