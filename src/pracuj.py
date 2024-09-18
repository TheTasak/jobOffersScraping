import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import (
    NoSuchElementException, StaleElementReferenceException, TimeoutException
)
from selenium.webdriver.common.by import By
import time
import os
from datetime import datetime, date

import src.transform
from src.utils import iterate_file, get_element_by_xpath, save_links

MAX_PAGES = 100
MAX_FILE_ITER = 10000
FILE_PATH = "pracuj/out.csv"
TRANSFORMED_FILE_PATH = f'pracuj/transformed_out-{date.today()}.csv'
INDEX_FILE_PATH = "pracuj/index.csv"


def scrap_links(max_pages: int = MAX_PAGES) -> pd.DataFrame:
    driver = webdriver.Firefox()

    data = {
        "id": [],
        "url": [],
        "created_at": [],
        "source": [],
    }

    index = 0
    for i in range(1, max_pages + 1):
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
                data["id"].append(index)
                data["url"].append(el_url)
                data["created_at"].append(datetime.now())
                data["source"].append("pracuj")
                num_of_elements += 1
                index += 1

        if num_of_elements == 0:
            break

    driver.quit()
    return pd.DataFrame.from_dict(data)


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    new_df = df.copy()
    rows_before = new_df.shape[0]

    new_df = new_df.drop_duplicates(subset=['url'], keep='first')
    # Remove url parameters
    new_df["url"] = new_df["url"].apply(lambda x: x[:x.find("?")])
    # Remove urls with wrong domain
    new_df = new_df[new_df["url"].str.contains("pracodawcy.pracuj.pl") == False]
    rows_after = new_df.shape[0]

    # print(f'Removed {rows_before - rows_after} rows')
    return new_df


def extract_posting_data(driver, data: dict, url: str, index: int) -> None:
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

    job_title_path = '//*[@data-test="text-positionName"]'
    job_title = get_element_by_xpath(driver, job_title_path)
    data["job_title"].append(job_title)

    company_path = '//*[@data-test="text-employerName"]'
    company = get_element_by_xpath(driver, company_path)
    company = company.replace('O firmie', '')
    company = company.replace('About the company', '')
    data["company"].append(company)

    salary_path = '//*[@data-test="section-salary"]'
    salary = get_element_by_xpath(driver, salary_path)
    data["salary"].append(salary)
    data["high_salary"].append("")
    data["low_salary"].append("")
    data["type_of_salary"].append("")

    location_path = '//*[@data-test="sections-benefit-workplaces"]/div[2]/div[2]'
    location = get_element_by_xpath(driver, location_path)
    data["location"].append(location)

    category_path = '//*[@data-test="nested-breadcrumb"]'
    try:
        element = driver.find_elements(By.XPATH, category_path)
        category = element[-1].text if len(element) > 0 else ""
    except (NoSuchElementException, StaleElementReferenceException) as e:
        category = ""
    data["category"].append(category)

    type_of_work_path = '//*[@data-test="sections-benefit-work-schedule"]'
    type_of_work = get_element_by_xpath(driver, type_of_work_path)
    experience_path = '//*[@data-test="sections-benefit-employment-type-name"]'
    experience = get_element_by_xpath(driver, experience_path)
    employment_type_path = '//*[@data-test="sections-benefit-contracts"]'
    employment_type = get_element_by_xpath(driver, employment_type_path)
    operating_mode_path = '//*[@data-scroll-id="work-modes"]'
    operating_mode = get_element_by_xpath(driver, operating_mode_path)

    data["type_of_work"].append(type_of_work)
    data["experience"].append(experience)
    data["employment_type"].append(employment_type)
    data["operating_mode"].append(operating_mode)

    skills_expected_path = '//*[@data-test="section-technologies-expected"]/div'
    skills_expected = get_element_by_xpath(driver, skills_expected_path)
    skills_optional_path = '//*[@data-test="section-technologies-optional"]/div'
    skills_optional = get_element_by_xpath(driver, skills_optional_path)
    skills = skills_expected + "\n" + skills_optional
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
    iterate_file(FILE_PATH, TRANSFORMED_FILE_PATH, INDEX_FILE_PATH, extract_posting_data, MAX_FILE_ITER)


def transform_links() -> None:
    df = pd.read_csv(INDEX_FILE_PATH, sep=",")
    df["category"] = df["category"].fillna("")
    df["category"] = df["category"].apply(lambda cat: "" if "Aplikuj" in str(cat) else cat)
    category_remove = [
        'Warszawa', 'Katowice', 'Lublin', 'Poznań', 'Łomża', 'Olsztyn', 'Otomin', 'Toruń', 'Kraków', 'Łódź', 'Gdańsk',
        'Gorzów Wielkopolski', 'Sokołów', 'Niemcy', 'Kielce', 'Koszalin', 'Świętochłowice', 'Wrocław', 'Bydgoszcz',
        'Opacz-Kolonia', 'Łomianki', 'Bielsko-Biała', 'Kunów', 'Grudziądz', 'Poniec', 'Pszczyna', 'Sosnowiec',
        'Trzcianka', 'Pruszcz Gdański', 'Zielona Góra', 'Siemianice', 'Tychy', 'Pietrzykowice', 'Sopot'
    ]
    df["category"] = df["category"].apply(lambda cat: "" if str(cat) in category_remove else cat)

    df["company"] = df["company"].fillna("")
    df["company"] = df["company"].str.replace("About the company", "")

    df["experience"] = df["experience"].fillna("")
    experience_map = {
        "mid": "regular", "regular": "regular", "senior": "senior", "junior": "junior", "expert": "senior",
        "ekspert": "senior", "manager": "c - level", "kierownik": "c - level", "dyrektor": "c - level",
        "praktykant": "trainee", "menedżer": "c - level", "director": "c - level", "assistant": "junior",
        "trainee": "trainee", "asystent": "trainee"
    }
    df["experience"] = df["experience"].apply(lambda exp: next((v for k, v in experience_map.items() if k in exp), exp))


    df["type_of_work"] = df["type_of_work"].fillna("")
    type_map = {
        "pełny etat": "full-time", "full-time": "full-time", "część etatu": "part-time", "part time": "part-time",
        "dodatkowa": "part-time", "additional": "part-time"
    }
    df["type_of_work"] = df["type_of_work"].apply(lambda type: next((v for k, v in type_map.items() if k in type), type))
    df.to_csv("check_result.csv", index=False, header=True, mode='w')


def etl() -> None:
    # extract
    links = scrap_links()
    save_links(FILE_PATH, links)

    links = remove_duplicates(links)
    save_links(FILE_PATH, links)

    iterate_links()

    # transform
    transform_links()

    # load
    status = transform.load_file_to_db(TRANSFORMED_FILE_PATH)
    print(f'LOADING PRACUJ TRANSFORM {"SUCCESSFUL" if status else "FAILED"}')
    status = transform.load_iterate_index_to_db(INDEX_FILE_PATH)
    print(f'LOADING PRACUJ INDEX {"SUCCESSFUL" if status else "FAILED"}')


if __name__ == '__main__':
    etl()
