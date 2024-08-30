from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException, ElementNotInteractableException, StaleElementReferenceException, TimeoutException
)
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import os
from datetime import datetime, date
import pandas

from utils import iterate_file, get_element_by_xpath

MAX_SCROLL = 100
MAX_FILE_ITER = 10000
FILE_PATH = "solidjobs/out.csv"
TRANSFORMED_FILE_PATH = f'solidjobs/transformed_out-{date.today()}.csv'
INDEX_FILE_PATH = "solidjobs/index.csv"


def scrap_links() -> None:
    urls = [
        "https://solid.jobs/offers/it;cities=Warszawa",
        "https://solid.jobs/offers/it;cities=Warszawa;categories=Data%20Science",
        "https://solid.jobs/offers/it;cities=Warszawa;categories=Pozostali%20specjali%C5%9Bci%20IT",
        "https://solid.jobs/offers/it;cities=Krak%C3%B3w",
        "https://solid.jobs/offers/it;cities=Wroc%C5%82aw",
        "https://solid.jobs/offers/it;cities=Pozna%C5%84",
        "https://solid.jobs/offers/it;cities=Katowice",
        "https://solid.jobs/offers/it;cities=Tr%C3%B3jmiasto",
        "https://solid.jobs/offers/it;cities=%C5%81%C3%B3d%C5%BA",
        "https://solid.jobs/offers/it;cities=Szczecin",
        "https://solid.jobs/offers/it;cities=Bydgoszcz",
        "https://solid.jobs/offers/it;cities=Lublin",
        "https://solid.jobs/offers/it;cities=Bia%C5%82ystok",
        "https://solid.jobs/offers/it;cities=Bielsko-Bia%C5%82a",
        "https://solid.jobs/offers/it;cities=Gliwice",
        "https://solid.jobs/offers/it;cities=Opole",
        "https://solid.jobs/offers/it;cities=Rzesz%C3%B3w",
        "https://solid.jobs/offers/it;cities=Praca%20zdalna",
        "https://solid.jobs/offers/it;cities=Inne"
    ]

    index = 1
    driver = webdriver.Firefox()

    for url in urls:
        data = {
            "id": [],
            "url": [],
            "created_at": [],
            "source": [],
        }

        driver.get(url)
        driver.implicitly_wait(3)

        while True:
            xpath = "//app-offer-list/div/div[1]/div/virtual-scroller/div[2]/div[1]/offer-list-item"
            xpath_scroll = "//app-offer-list/div/div[1]/div/virtual-scroller"
            try:
                scroll = driver.find_element(By.XPATH, value=xpath_scroll)
                link_elements = driver.find_elements(By.XPATH, value=xpath)
            except (NoSuchElementException, StaleElementReferenceException) as e:
                break
            else:
                duplicates = False
                for el in link_elements:
                    try:
                        url = el.find_element(By.TAG_NAME, "a").get_attribute("href")
                    except (NoSuchElementException, StaleElementReferenceException) as e:
                        url = ""
                    else:
                        diff = len(data["url"]) - len(link_elements)
                        if len(data["url"]) > len(link_elements) and data["url"][diff] == url:
                            duplicates = True
                            break
                    data["id"].append(index)
                    data["url"].append(url)
                    data["created_at"].append(datetime.now())
                    data["source"].append("solid.jobs")
                    index += 1

                if duplicates:
                    break

                try:
                    for _ in range(8):
                        scroll.send_keys(Keys.PAGE_DOWN)
                except ElementNotInteractableException:
                    break
                time.sleep(1)

        df = pandas.DataFrame.from_dict(data)
        df = df.drop_duplicates(subset=['url'], keep='first')
        df.to_csv(FILE_PATH, index=False, mode='a', header=not os.path.exists(FILE_PATH))
    driver.quit()


def remove_duplicates() -> None:
    df = pandas.read_csv(FILE_PATH, sep=",")
    rows_before = df.shape[0]

    df = df.drop_duplicates(subset=['url'], keep='first')
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

    driver.implicitly_wait(1)

    data["id"].append(index)
    data["url"].append(url)

    job_title_path = "//offer-details-header/div/div/div[2]/div[1]"
    job_title = get_element_by_xpath(driver, job_title_path)
    data["job_title"].append(job_title)

    company_path = "//offer-details-header/div/div/div[2]/div[2]/div/span[1]"
    company = get_element_by_xpath(driver, company_path)
    data["company"].append(company.strip())

    location_path = "//offer-details-header/div/div/div[2]/div[2]/div/span[2]"
    location = get_element_by_xpath(driver, location_path)
    data["location"].append(location.strip())

    salary_path = '//offer-details-header/div/div/div[3]/div[1]'
    salary = get_element_by_xpath(driver, salary_path)
    data["salary"].append(salary)

    category_path = '//offer-details-header/div/div/div[2]/div[3]/solidjobs-category-display'
    category = get_element_by_xpath(driver, category_path)
    data["category"].append(category)

    employment_type_path = '//offer-details-salary/div/div/div[2]/div[1]/div[2]/div/div/p'
    type_of_work_path = '//offer-details-salary/div/div/div[2]/div[2]/div[1]/div/div/p'
    experience_path = '//solidjobs-category-display/a'

    employment_type = get_element_by_xpath(driver, employment_type_path)
    type_of_work = get_element_by_xpath(driver, type_of_work_path)

    try:
        element = driver.find_element(By.XPATH, value=experience_path)
        experience = element.get_attribute("class")
    except (NoSuchElementException, StaleElementReferenceException) as e:
        experience = ""

    if "100% zdalnie" in location:
        operating_mode = "remote"
    else:
        operating_mode = "other"

    if "badge-regular" in experience:
        experience = "regular"
    elif "badge-senior" in experience:
        experience = "senior"
    elif "badge-junior" in experience:
        experience = "junior"
    else:
        experience = ""
    data["type_of_work"].append(type_of_work)
    data["experience"].append(experience)
    data["employment_type"].append(employment_type)
    data["operating_mode"].append(operating_mode)

    skills_path = '//offer-details-header/div/div/div[2]/div[3]/solidjobs-skill-display-advanced'
    levels = ["junior", "regular", "advanced"]
    try:
        elements = driver.find_elements(By.XPATH, value=skills_path)
        skills = ""
        for element in elements:
            try:
                skill_level = element.find_elements(By.CLASS_NAME, "fa-circle")
            except (NoSuchElementException, StaleElementReferenceException) as e:
                skills += element.text + ";"
                continue
            else:
                if 0 < len(skill_level) < 4:
                    level = levels[len(skill_level)-1]
                else:
                    level = ""
            skills += element.text + "\n" + level + ";"
    except (NoSuchElementException, StaleElementReferenceException) as e:
        skills = ""
    data["skills"].append(skills)

    description_path = '//offer-details-details'
    description_second_path = '//offer-details-profile'
    description = get_element_by_xpath(driver, description_path)
    description += get_element_by_xpath(driver, description_second_path)

    data["description"].append(description)


def iterate_links() -> None:
    iterate_file(FILE_PATH, TRANSFORMED_FILE_PATH, INDEX_FILE_PATH, extract_posting_data, MAX_FILE_ITER)


def etl() -> None:
    scrap_links()
    remove_duplicates()
    iterate_links()


if __name__ == '__main__':
    etl()
