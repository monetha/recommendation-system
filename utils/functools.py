import pandas as pd
from joblib import Parallel, delayed
import multiprocessing
from itertools import tee, islice, chain

def applyParallel(dfGrouped, func, *kwards):
    retLst = Parallel(n_jobs=multiprocessing.cpu_count())(delayed(func)(group,*kwards) for name, group in dfGrouped)
    return pd.concat(retLst)

def split(a, t):
    k, m = divmod(len(a), t)
    return (a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(t))

def current_next(some_iterable):
    prevs, items, nexts = tee(some_iterable, 3)
    nexts = chain(islice(nexts, 1, None), [None])
    return zip(items, nexts)