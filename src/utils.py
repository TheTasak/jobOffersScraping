import pandas
import copy
import datetime
import os
import time
from datetime import datetime, timedelta

import pandas as pd
from selenium import webdriver
from selenium.common import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.by import By

DEFAULT_INDEX_TEMPLATE = {
    "id": [],
    "url": [],
    "job_title": [],
    "location": [],
    "category": [],
    "company": [],
    "salary": [],
    "type_of_salary": [],
    "low_salary": [],
    "high_salary": [],
    "type_of_work": [],
    "experience": [],
    "employment_type": [],
    "operating_mode": [],
    "skills": [],
    "description": [],
}

DEFAULT_WRITE_TEMPLATE = {
    "id": [],
    "created_at": [],
}


def find_nth(haystack: str, needle: str, n: int) -> int:
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start+len(needle))
        n -= 1
    return start


def save_links(save_file: str, data: pd.DataFrame):
    path = save_file.split("/")
    if len(path) > 1 and not os.path.exists(path[0]):
        os.mkdir(path[0])
    data.to_csv(save_file, index=False, mode='w')


def format_time(seconds: float) -> str:
    if seconds < 60:
        return f'{round(seconds, 2)} seconds'
    elif seconds < 60 * 60:
        return f'{round(seconds / 60, 2)} minutes'
    else:
        return f'{round(seconds / 3600, 2)} hours'


def map_dict_to_column(cell: str, value_dict: dict):
    for key, value in value_dict.items():
        if key in cell:
            return value
    return cell



def get_element_by_xpath(driver, path: str) -> str:
    try:
        element = driver.find_element(By.XPATH, value=path)
    except (NoSuchElementException, StaleElementReferenceException) as e:
        return ""
    else:
        return element.text


def save_data(write_file: str, data: any, mode='a') -> None:
    new_df = pandas.DataFrame.from_dict(data)
    new_df.to_csv(write_file,
                  index=False,
                  header=not os.path.exists(write_file),
                  mode=mode)


def iterate_file(read_file: str, write_file: str, index_file: str, iterate_func, max_file_iterations=10000, template=None) -> None:
    if template is None:
        template = DEFAULT_INDEX_TEMPLATE

    driver = webdriver.Firefox()
    df = pandas.read_csv(read_file, sep=",")
    max_file_iterations = df.shape[0] if df.shape[0] < max_file_iterations else max_file_iterations
    print("STARTED SCANNING...")

    i = 0
    sum_time = 0
    last_time = datetime.now()

    try:
        index_df = pandas.read_csv(index_file, sep=",")
    except (FileNotFoundError, pandas.errors.EmptyDataError) as e:
        index_df = pd.DataFrame(columns=['id', 'url'])
        index_i = 0
    else:
        index_i = index_df["id"].max() + 1

    index_data = copy.deepcopy(template)

    write_data = copy.deepcopy(DEFAULT_WRITE_TEMPLATE)

    for index, row in df.iterrows():
        if i == max_file_iterations:
            break

        # check if url exists in index, optimize by skipping scanning of the url
        found_rows = index_df[index_df["url"] == row["url"]]
        if len(found_rows) > 0:
            write_data["id"].append(found_rows["id"].iloc[0])
            write_data["created_at"].append(datetime.now())
        else:
            write_data["id"].append(index_i)
            write_data["created_at"].append(datetime.now())
            iterate_func(driver, index_data, row["url"], index_i)
            time.sleep(2)
            index_i += 1
        i += 1

        print(f'{datetime.now()} [{i}/{max_file_iterations}] SCANNED: {round(i / max_file_iterations * 100, 2)}%')

        delta = datetime.now() - last_time
        sum_time += delta.seconds
        last_time = datetime.now()

        if i % 10 == 0:
            diff_seconds = (sum_time / i) * (max_file_iterations - i)
            end_date = datetime.now() + timedelta(seconds=diff_seconds)
            print(f'\nETC: {end_date} - {format_time(diff_seconds)} left\n')

        if i % 100 == 0:
            save_data(index_file, index_data)
            index_data = copy.deepcopy(template)

            save_data(write_file, write_data)
            write_data = copy.deepcopy(DEFAULT_WRITE_TEMPLATE)

            time.sleep(5)

    save_data(index_file, index_data)
    save_data(write_file, write_data)
    driver.quit()
