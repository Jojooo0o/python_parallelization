import multiprocessing as mp
import numpy as np
from time import time

def main():
    print('Number of processors: ', mp.cpu_count())

    np.random.RandomState(100)
    arr = np.random.randint(0, 10, size=[200000, 5])
    data = arr.tolist()
    data[:5]

    # non parallelized
    results = []
    for row in data:
        results.append(howmany_within_range(row, minimum=4, maximum=8))

    print(results[:10])

    # pool.apply parallelized
    pool = mp.Pool(mp.cpu_count())

    results = [pool.apply(howmany_within_range, args=(row, 4, 8)) for row in data]

    pool.close()

    print(results[:10])

    # pool.map parallelized
    pool = mp.Pool(mp.cpu_count())
    results = pool.map(howmany_within_range_map, [row for row in data])

    pool.close()

    print(results[:10])

    # pool.starmap parallelized
    pool = mp.Pool(mp.cpu_count())
    results = pool.starmap(howmany_within_range, [(row, 4, 8) for row in data])

    pool.close()



# non parallelized
def howmany_within_range(row, minimum, maximum):
    count = 0
    for n in row:
        if minimum <= n <= maximum:
            count = count + 1
    return count

# redefine to fit map (only allows one argument)
def howmany_within_range_map(row, minimum=4, maximum=8):
    count = 0
    for n in row:
        if minimum <= n <= maximum:
            count = count + 1
    return count





if __name__ == '__main__':
    main()
