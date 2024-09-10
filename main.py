import nofluffjobs as nfj
import justjoinit as jji
import solidjobs as sj
import pracuj as p

import threading
from pprint import pprint
import transform


def threads(pracuj=False, solidjobs=False, justjoin=False):
    thread_arr = []
    if pracuj:
        thread_arr.append(threading.Thread(target=p.etl))
    if solidjobs:
        thread_arr.append(threading.Thread(target=sj.etl))
    if justjoin:
        thread_arr.append(threading.Thread(target=jji.etl))

    for thread in thread_arr:
        thread.start()
    for thread in thread_arr:
        thread.join()
    print("DONE")


def save_occurrences(pracuj=False, solidjobs=False, justjoin=False):
    successful = "FINISHED SUCCESSFULLY"
    failed = "FAILED"
    if pracuj:
        status = transform.load_file_to_db(p.TRANSFORMED_FILE_PATH)
        print(f'LOADING JUSTJOIN TRANSFORM {successful if status else failed}')
    if solidjobs:
        status = transform.load_file_to_db(sj.TRANSFORMED_FILE_PATH)
        print(f'LOADING JUSTJOIN TRANSFORM {successful if status else failed}')
    if justjoin:
        status = transform.load_file_to_db(jji.TRANSFORMED_FILE_PATH)
        print(f'LOADING JUSTJOIN TRANSFORM {successful if status else failed}')


def save_index(pracuj=False, solidjobs=False, justjoin=False):
    successful = "FINISHED SUCCESSFULLY"
    failed = "FAILED"
    if pracuj:
        status = transform.load_iterate_index_to_db(p.INDEX_FILE_PATH)
        print(f'LOADING PRACUJ INDEX {successful if status else failed}')
    if solidjobs:
        status = transform.load_iterate_index_to_db(sj.INDEX_FILE_PATH)
        print(f'LOADING SOLIDJOBS INDEX {successful if status else failed}')
    if justjoin:
        status = transform.load_iterate_index_to_db(jji.INDEX_FILE_PATH)
        print(f'LOADING JUSTJOIN INDEX {successful if status else failed}')


if __name__ == '__main__':
    # threads(pracuj=True, justjoin=True)
    # threads(solidjobs=True)
    # save_index(pracuj=True, solidjobs=True, justjoin=True)
    # transform.load_iterate_index_to_db("pracuj/index.csv")
    pprint(transform.search_term("airflow", "en"))


