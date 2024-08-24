from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException, ElementNotInteractableException, StaleElementReferenceException, TimeoutException
)
from selenium.webdriver.common.by import By
import time
import os
from datetime import datetime
import pandas

from utils import iterate_file, get_element_by_xpath

MAX_PAGES = 100
MAX_FILE_ITER = 1000
FILE_PATH = "pracuj/out.csv"
TRANSFORMED_FILE_PATH = "pracuj/transformed_out.csv"


def scrap_links() -> None:
    driver = webdriver.Firefox()

    data = {
        "id": [],
        "url": [],
        "created_at": [],
        "source": [],
    }

    for i in range(1, MAX_PAGES + 1):
        url = f'https://it.pracuj.pl/praca?pn={i}'

        driver.get(url)
        driver.implicitly_wait(2)

        xpath = '//*[@id="offers-list"]/div[3]/div'
        try:
            elements = driver.find_elements(By.XPATH, value=xpath)
        except (NoSuchElementException, StaleElementReferenceException) as e:
            continue

        num_of_elements = 0
        for el in elements:
            try:
                el_url = el.find_element(By.TAG_NAME, "a").get_attribute("href")
            except (NoSuchElementException, StaleElementReferenceException) as e:
                continue
            else:
                num_of_elements += 1
                data["id"].append(i)
                data["url"].append(el_url)
                data["created_at"].append(datetime.now())
                data["source"].append("pracuj.pl")

        if num_of_elements == 0:
            break

    df = pandas.DataFrame.from_dict(data)
    df = df.drop_duplicates(subset=['url'], keep='first')

    path = FILE_PATH.split("/")
    if len(path) > 1 and not os.path.exists(path[0]):
        os.mkdir(path[0])
    df.to_csv(FILE_PATH, index=False, mode='a', header=not os.path.exists(FILE_PATH))

    driver.quit()


def remove_duplicates() -> None:
    df = pandas.read_csv(FILE_PATH, sep=",")
    rows_before = df.shape[0]

    df = df[df["url"].str.contains("pracodawcy.pracuj.pl") == False]
    rows_after = df.shape[0]

    df.to_csv(FILE_PATH, index=False)
    print(f'Removed {rows_before - rows_after} rows')


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

    job_title_path = '//*[@id="offer-details"]/div[1]/div[1]/div[1]/div[1]/div[2]/h1'
    job_title = get_element_by_xpath(driver, job_title_path)
    data["job_title"].append(job_title)

    company_path = '//*[@id="offer-details"]/div[1]/div[1]/div[1]/div[1]/div[2]/h2'
    company = get_element_by_xpath(driver, company_path)
    company = company.replace('O firmie', '')
    data["company"].append(company)

    salary_path = '//*[@data-test="section-salary"]'
    salary = get_element_by_xpath(driver, salary_path)
    data["salary"].append(salary)

    location_path = '//*[@id="offer-details"]/div[1]/div[1]/ul[1]/li[1]/div[2]'
    location = get_element_by_xpath(driver, location_path)
    data["location"].append(location)

    category_path = '//*[@id="offer-details"]/div[1]/div[4]/div/div'
    try:
        element = driver.find_elements(By.XPATH, category_path)
        category = element[-1].text if len(element) > 0 else ""
    except (NoSuchElementException, StaleElementReferenceException) as e:
        category = ""
    data["category"].append(category)

    type_of_work_path = '//*[@id="offer-details"]/div[1]/div[1]/ul[1]/li[4]/div[2]'
    type_of_work = get_element_by_xpath(driver, type_of_work_path)
    experience_path = '//*[@id="offer-details"]/div[1]/div[1]/ul[1]/li[5]/div[2]'
    experience = get_element_by_xpath(driver, experience_path)
    employment_type_path = '//*[@id="offer-details"]/div[1]/div[1]/ul[1]/li[3]/div[2]'
    employment_type = get_element_by_xpath(driver, employment_type_path)
    operating_mode_path = '//*[@id="offer-details"]/div[1]/div[1]/ul[1]/li[6]/div[2]'
    operating_mode = get_element_by_xpath(driver, operating_mode_path)

    data["type_of_work"].append(type_of_work)
    data["experience"].append(experience)
    data["employment_type"].append(employment_type)
    data["operating_mode"].append(operating_mode)

    skills_expected_path = '//*[@data-test="section-technologies-expected"]/div'
    skills_expected = get_element_by_xpath(driver, skills_expected_path)
    skills_optional_path = '//*[@data-test="section-technologies-optional"]/div'
    skills_optional = get_element_by_xpath(driver, skills_optional_path)
    skills = skills_expected + " " + skills_optional
    data["skills"].append(skills)

    description_1_path = '//*[@data-test="section-about-project"]'
    description_2_path = '//*[@data-test="section-responsibilities"]'
    description_3_path = '//*[@data-test="section-requirements"]'
    description_4_path = '//*[@data-test="section-training-space"]'
    description_5_path = '//*[@data-test="section-offered"]'
    description_6_path = '//*[@data-test="section-benefits"]'
    description_1 = get_element_by_xpath(driver, description_1_path)
    description_2 = get_element_by_xpath(driver, description_2_path)
    description_3 = get_element_by_xpath(driver, description_3_path)
    description_4 = get_element_by_xpath(driver, description_4_path)
    description_5 = get_element_by_xpath(driver, description_5_path)
    description_6 = get_element_by_xpath(driver, description_6_path)
    description = "\n".join([description_1, description_2, description_3, description_4, description_5, description_6])
    data["description"].append(description)


def iterate_links() -> None:
    iterate_file(FILE_PATH, TRANSFORMED_FILE_PATH, extract_posting_data, MAX_FILE_ITER)


def etl() -> None:
    scrap_links()
    remove_duplicates()
    iterate_links()


if __name__ == '__main__':
    etl()
