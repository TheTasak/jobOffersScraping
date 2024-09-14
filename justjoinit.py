import pandas
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
from selenium.webdriver.common.by import By
import time
from datetime import datetime, date
import os

import transform
from utils import iterate_file, get_element_by_xpath


MAX_SCROLL = 150
MAX_FILE_ITER = 10000
FILE_PATH = "justjoin/out.csv"
TRANSFORMED_FILE_PATH = f'justjoin/transformed_out-{date.today()}.csv'
INDEX_FILE_PATH = "justjoin/index.csv"


def scrap_links() -> None:
    url = "https://justjoin.it"
    index = 1
    data = {
        "id": [],
        "url": [],
        "created_at": [],
        "source": [],
    }

    driver = webdriver.Firefox()

    driver.get(url)
    driver.implicitly_wait(0.5)

    tried_scrolling = False
    scroll_amount = 1650
    current_scroll = scroll_amount

    while True:
        xpath = (f'//*[@id="__next"]/div[2]/div[1]/div/div[2]/div/div/div[2]/div/div[2]/div[@data-index="{index}"]/div'
                 f'/div/a')
        try:
            link_elements = driver.find_element(By.XPATH, value=xpath)
        except NoSuchElementException:
            if not tried_scrolling and current_scroll / scroll_amount < MAX_SCROLL:
                tried_scrolling = True
            else:
                break
            driver.execute_script(f"window.scrollTo(0, {current_scroll})")
            current_scroll += scroll_amount
            time.sleep(3)
        else:
            tried_scrolling = False
            data["id"].append(index)
            data["url"].append(link_elements.get_attribute("href"))
            data["created_at"].append(datetime.now())
            data["source"].append("justjoin")
            index += 1

    driver.quit()
    df = pandas.DataFrame.from_dict(data)

    path = FILE_PATH.split("/")
    if len(path) > 1 and not os.path.exists(path[0]):
        os.mkdir(path[0])
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

    job_title_path = '//*[@id="__next"]/div[2]/div/div/div[2]/div[2]/div[1]/div[2]/div[2]/h1'
    job_title = get_element_by_xpath(driver, job_title_path)
    data["job_title"].append(job_title)

    company_path = '//*[@id="__next"]/div[2]/div/div/div[2]/div[2]/div[1]/div[2]/div[2]/div[1]/div[1]'
    company = get_element_by_xpath(driver, company_path)
    data["company"].append(company)

    salary_path = '//*[@id="__next"]/div[2]/div/div/div[2]/div[2]/div[1]/div[2]/div[2]/div[2]/div/div'
    try:
        element = driver.find_elements(By.XPATH, value=salary_path)
        salary = ";".join([el.text for el in element])
    except (NoSuchElementException, StaleElementReferenceException) as e:
        salary = ""
    data["salary"].append(salary)
    data["high_salary"].append("")
    data["low_salary"].append("")
    data["type_of_salary"].append("")

    widgets_path = '//*[@id="__next"]/div[2]/div/div/div[2]/div[2]/div[2]/div'
    try:
        element = driver.find_elements(By.XPATH, value=widgets_path)
        widgets = [el.text for el in element]
        type_of_work = widgets[0].split("\n")[1].lower()
        experience = widgets[1].split("\n")[1].lower()
        employment_type = widgets[2].split("\n")[1].lower()
        operating_mode = widgets[3].split("\n")[1].lower()
    except (NoSuchElementException, StaleElementReferenceException, IndexError) as e:
        type_of_work = ""
        experience = ""
        employment_type = ""
        operating_mode = ""
    data["type_of_work"].append(type_of_work)
    data["experience"].append(experience)
    data["employment_type"].append(employment_type)
    data["operating_mode"].append(operating_mode)

    check_num_of_divs = '//*[@id="__next"]/div[2]/div/div/div[2]/div[2]/div'
    try:
        element = driver.find_elements(By.XPATH, value=check_num_of_divs)
        has_company_widget = len(element) == 7
        offset = 1 if has_company_widget else 0
    except (NoSuchElementException, StaleElementReferenceException) as e:
        offset = 1

    skills_path = f'//*[@id="__next"]/div[2]/div/div/div[2]/div[2]/div[{3 + offset}]/div/ul/div'
    try:
        element = driver.find_elements(By.XPATH, value=skills_path)
        skills = ";".join([el.text for el in element])
    except (NoSuchElementException, StaleElementReferenceException) as e:
        skills = ""
    data["skills"].append(skills)

    description_path = f'//*[@id="__next"]/div[2]/div/div/div[2]/div[2]/div[{4 + offset}]/div[2]'
    description = get_element_by_xpath(driver, description_path)
    data["description"].append(description)

    location_path = f'//*[@id="__next"]/div[2]/div/div/div[1]/div/a[2]'
    location = get_element_by_xpath(driver, location_path)
    data["location"].append(location)

    category_path = f'//*[@id="__next"]/div[2]/div/div/div[1]/div/a'
    try:
        elements = driver.find_elements(By.XPATH, value=category_path)
        category = elements[-1].text
    except (NoSuchElementException, StaleElementReferenceException, IndexError) as e:
        category = ""

    data["category"].append(category)


def iterate_links() -> None:
    iterate_file(FILE_PATH, TRANSFORMED_FILE_PATH, INDEX_FILE_PATH, extract_posting_data, MAX_FILE_ITER)


def transform_links() -> None:
    df = pandas.read_csv(INDEX_FILE_PATH, sep=",")

    df["salary"] = df["salary"].fillna("")
    df["type_of_salary"] = df.apply(lambda row: "netto" if str(row["salary"]).find("Net") != -1 else "brutto", axis=1)
    df["salary_index"] = df["salary"].str.rfind("PLN")
    df["salary"] = df.apply(
        lambda row: row["salary"][:int(row["salary_index"])] if row["salary_index"] != -1 else "",
        axis=1
    )
    df["low_salary"] = df.apply(lambda row: str(row["salary"]).split("-")[0].replace(" ", ""), axis=1)
    df["high_salary"] = df.apply(lambda row: str(row["salary"]).split("-")[-1].replace(" ", ""), axis=1)
    df = df.drop(columns=["salary", "salary_index"])

    df["type_of_work"] = df["type_of_work"].fillna("")
    df["type_of_work"] = df["type_of_work"].str.replace("jobtype.undetermined", "")

    df["experience"] = df["experience"].fillna("")
    df["experience"] = df["experience"].str.replace("mid", "regular")

    df["operating_mode"] = df["operating_mode"].fillna("")

    df["employment_type_temp"] = df["employment_type"].fillna("")
    df["employment_type"] = ""
    df["employment_type"] += df.apply(
        lambda row: "B2B, " if "b2b" in row["employment_type_temp"] else "",
        axis=1
    )
    df["employment_type"] += df.apply(
        lambda row: "Umowa o pracę, " if "permanent" in row["employment_type_temp"] else "",
        axis=1
    )
    df["employment_type"] += df.apply(
        lambda row: "Umowa zlecenie, " if "mandate" in row["employment_type_temp"] else "",
        axis=1
    )
    df["employment_type_index"] = df["employment_type"].str.rfind(",")
    df["employment_type"] = df.apply(
        lambda row: row["employment_type"][:int(row["employment_type_index"])] if row["employment_type_index"] != -1 else "",
        axis=1
    )
    df["employment_type"] = df.apply(
        lambda row: row["employment_type"] if len(row["employment_type"]) > 0 else row["employment_type_temp"],
        axis=1
    )

    df = df.drop(columns=["employment_type_temp", "employment_type_index"])
    print(df["employment_type"].unique())


def etl() -> None:
    # extract
    scrap_links()
    iterate_links()

    # transform
    transform_links()

    # load
    status = transform.load_file_to_db(TRANSFORMED_FILE_PATH)
    print(f'LOADING JUSTJOIN TRANSFORM {"SUCCESSFUL" if status else "FAILED"}')
    status = transform.load_iterate_index_to_db(INDEX_FILE_PATH)
    print(f'LOADING JUSTJOIN INDEX {"SUCCESSFUL" if status else "FAILED"}')


if __name__ == '__main__':
    etl()
