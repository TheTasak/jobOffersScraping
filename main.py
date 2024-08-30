import nofluffjobs as nfj
import justjoinit as jji
import solidjobs as sj
import pracuj as p

import threading

import transform


def threads(pracuj=False, solidjobs=False):
    thread_arr = []
    if pracuj:
        thread_arr.append(threading.Thread(target=p.etl))
    if solidjobs:
        thread_arr.append(threading.Thread(target=sj.etl))

    for thread in thread_arr:
        thread.start()
    for thread in thread_arr:
        thread.join()
    print("DONE")


if __name__ == '__main__':
    threads(pracuj=True, solidjobs=True)


