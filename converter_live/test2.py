from datetime import datetime
import time
from datetime import datetime

now = datetime.now()
seconds_since_midnight = (now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()



n = 18
ciao1 = (time.time() - seconds_since_midnight) - 86400*(n)
ciao2 = (time.time() - seconds_since_midnight) - 86400*(n-1)

ciao = 1523291495.0


if ciao<ciao2 and ciao>ciao1:
    print "OK"


