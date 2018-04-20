from astropy.table import Table, Column
import numpy as np

t = Table(names=('a', 'b', 'c'), dtype=('f', 'i', 'S'))
t.add_row((1, 2.0, 'x'))
t.add_row((4, 5.0, 'y'))

print t