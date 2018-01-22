
import sys

if sys.version_info[0] > 2:
    from fastprocesspool.fastprocesspool import Pool
else:
    from fastprocesspool import Pool
