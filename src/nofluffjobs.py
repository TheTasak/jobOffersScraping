import os
import time

import pandas
from datetime import date, datetime
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
from selenium.webdriver.common.by import By

import src.transform
from src.utils import iterate_file, get_element_by_xpath

MAX_CLICK = 10
MAX_FILE_ITER = 5
FILE_PATH = "nofluffjobs/out.csv"
TRANSFORMED_FILE_PATH = f'nofluffjobs/transformed_out-{date.today()}.csv'
INDEX_FILE_PATH = "nofluffjobs/index.csv"


def scrap_links() -> None:
    url = "https://nofluffjobs.com/pl"
    data = {
        "id": [],
        "url": [],
        "created_at": [],
        "source": [],
    }
    index = 0
    scroll_amount = 2000
    current_scroll = scroll_amount

    driver = webdriver.Firefox()

    driver.get(url)
    driver.implicitly_wait(0.5)

    while True:
        xpath = '//*[@listname="homepage_below_rozchodniak"]/div/a'

        try:
            link_elements = driver.find_elements(By.XPATH, value=xpath)
        except (NoSuchElementException, StaleElementReferenceException) as e:
            link_elements = []
        else:
            link_elements = [el.get_property("href") for el in link_elements]

        for link in link_elements:
            data["id"].append(index)
            data["url"].append(link)
            data["created_at"].append(datetime.now())
            data["source"].append("nofluffjobs")
            index += 1

        button_xpath = '//nfj-homepage-listings/div[2]/button'
        try:
            button = driver.find_element(By.XPATH, value=button_xpath)
        except (NoSuchElementException, StaleElementReferenceException) as e:
            break
        else:
            driver.execute_script(f"window.scrollTo(0, {current_scroll})")
            current_scroll += scroll_amount
            driver.execute_script("arguments[0].click();", button)
            time.sleep(2)

    driver.close()

    df = pandas.DataFrame.from_dict(data)

    path = FILE_PATH.split("/")
    if len(path) > 1 and not os.path.exists(path[0]):
        os.mkdir(path[0])
    df = df.drop_duplicates(subset=["url"], keep="first")
    df.to_csv(FILE_PATH, index=False, mode='w')


def extract_posting_data(driver, data, url, index) -> None:
    try:
        driver.get(url)
    except TimeoutException:
        time.sleep(5)
    try:
        driver.get(url)
    except TimeoutException:
        return
    driver.implicitly_wait(0.5)

    data["id"].append(index)
    data["url"].append(url)

    job_title_path = '//common-posting-header/div/div/h1'
    job_title = get_element_by_xpath(driver, job_title_path)
    data["job_title"].append(job_title)

    company_path = '//common-posting-header/div/div/a'
    company = get_element_by_xpath(driver, company_path)
    data["company"].append(company)

    salary_path = '//common-posting-salaries-list'
    salary = get_element_by_xpath(driver, salary_path)
    data["salary"].append(salary)

    experience_path = '//common-posting-content-wrapper/div/section[1]/ul/li[2]/div/span'
    experience = get_element_by_xpath(driver, experience_path)
    data["experience"].append(experience)

    category_path = '//common-posting-content-wrapper/div/section[1]/ul/li[1]/div/aside/div'
    category = get_element_by_xpath(driver, category_path)
    data["category"].append(category)

    type_of_work = ""
    employment_type = ""
    operating_mode = ""
    data["type_of_work"].append(type_of_work)
    data["employment_type"].append(employment_type)
    data["operating_mode"].append(operating_mode)

    location_path = '//common-posting-locations'
    location = get_element_by_xpath(driver, location_path)
    data["location"].append(location)

    skills_required_path = '//*[@id="posting-requirements"]/section[1]/ul/li'
    skills_optional_path = '//*[@id="posting-requirements"]/section[2]/ul/li'
    try:
        elements = driver.find_elements(By.XPATH, value=skills_required_path)
        skills_required = ";".join([el.text for el in elements])
    except (NoSuchElementException, StaleElementReferenceException) as e:
        skills_required = ""

    try:
        elements = driver.find_elements(By.XPATH, value=skills_optional_path)
        skills_optional = ";".join([el.text for el in elements])
    except (NoSuchElementException, StaleElementReferenceException) as e:
        skills_optional = ""

    data["skills"].append("\n".join([skills_required, skills_optional]))

    # description_path = f'//*[@id="__next"]/div[2]/div/div/div[2]/div[2]/div[{4 + offset}]/div[2]'
    # description = get_element_by_xpath(driver, description_path)
    description = ""
    data["description"].append(description)


def iterate_links() -> None:
    iterate_file(FILE_PATH, TRANSFORMED_FILE_PATH, INDEX_FILE_PATH, extract_posting_data, MAX_FILE_ITER)


def etl() -> None:
    scrap_links()
    iterate_links()
    status = transform.load_file_to_db(TRANSFORMED_FILE_PATH)
    print(f'LOADING NOFLUFFJOBS TRANSFORM {"SUCCESSFUL" if status else "FAILED"}')
    status = transform.load_iterate_index_to_db(INDEX_FILE_PATH)
    print(f'LOADING NOFLUFFJOBS INDEX {"SUCCESSFUL" if status else "FAILED"}')
