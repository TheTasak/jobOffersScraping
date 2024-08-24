import pandas
import copy
import datetime
import os
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.common import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.by import By

DEFAULT_TEMPLATE = {
    "id": [],
    "url": [],
    "job_title": [],
    "location": [],
    "category": [],
    "company": [],
    "salary": [],
    "type_of_work": [],
    "experience": [],
    "employment_type": [],
    "operating_mode": [],
    "skills": [],
    "description": [],
}


def format_time(seconds) -> str:
    if seconds < 60:
        return f'{seconds} seconds'
    elif seconds < 60 * 60:
        return f'{round(seconds / 60, 2)} minutes'
    else:
        return f'{round(seconds / 3600, 2)} hours'


def get_element_by_xpath(driver, path) -> str:
    try:
        element = driver.find_element(By.XPATH, value=path)
    except (NoSuchElementException, StaleElementReferenceException) as e:
        return ""
    return element.text


def save_data(write_file, data) -> None:
    new_df = pandas.DataFrame.from_dict(data)
    new_df.to_csv(write_file,
                  index=False,
                  header=not os.path.exists(write_file),
                  mode='a')


def iterate_file(read_file, write_file, iterate_func, max_file_iterations=10000, template=None) -> None:
    if template is None:
        template = DEFAULT_TEMPLATE

    driver = webdriver.Firefox()
    df = pandas.read_csv(read_file, sep=",")
    data = copy.deepcopy(template)
    print("STARTED SCANNING...")

    i = 0
    sum_time = 0
    last_time = datetime.now()

    for index, row in df.iterrows():
        if i == max_file_iterations:
            break

        iterate_func(driver, data, row["url"], i)
        i += 1

        print(f'{datetime.now()} [{i}/{max_file_iterations}] SCANNED: {round(i / max_file_iterations * 100, 2)}%')
        time.sleep(2)

        delta = datetime.now() - last_time
        sum_time += delta.seconds
        last_time = datetime.now()

        if i % 2 == 0:
            diff_seconds = (sum_time / i) * (max_file_iterations - i)
            end_date = datetime.now() + timedelta(seconds=diff_seconds)
            print(f'\nETC: {end_date} - {format_time(diff_seconds)} left\n')

        if i % 100 == 0:
            save_data(write_file, data)
            data = copy.deepcopy(template)
            time.sleep(8)
    save_data(write_file, data)
    driver.quit()
