import os

import requests
import pandas
from datetime import date
import time

START_PAGE = 0
MAX_PAGE = 100


def get_id_suffix(last_posting, current_posting) -> str:
    last_index = 0
    for index, element in enumerate(last_posting):
        if len(current_posting) > index and element == current_posting[index]:
            last_index += 1
    return current_posting[last_index:]


def extract_postings(response) -> int:
    postings = response["postings"]
    data = {
        "id": [],
        "locations": [],
        "company": [],
        "name": [],
        "posted": [],
        "renewed": [],
        "remote": [],
        "category": [],
        "seniority": [],
        "salary_from": [],
        "salary_to": [],
        "employment_type": []
    }
    for posting in postings:
        is_variant = False
        suffix = ""
        if len(data["id"]) > 0:
            suffix = get_id_suffix(data["id"][-1].split("-"), posting["id"].split("-"))
            # TODO: create a more advanced solution to detecting duplicate offers
            is_variant = len(suffix) < (len(posting["id"].split("-")) / 5) * 3
            suffix = "-".join(suffix)

        if is_variant:
            # data["locations"][-1] += ";" + suffix
            continue

        data["id"].append(None if "id" not in posting else posting["id"])
        data["locations"].append("")
        data["company"].append(None if "name" not in posting else posting["name"])
        data["name"].append(None if "title" not in posting else posting["title"])
        data["posted"].append(None if "posted" not in posting else posting["posted"])
        data["renewed"].append(None if "renewed" not in posting else posting["renewed"])
        data["remote"].append(None if "fullyRemote" not in posting else posting["fullyRemote"])
        data["category"].append(None if "category" not in posting else posting["category"])
        data["seniority"].append(None if "seniority" not in posting else "|".join(posting["seniority"]))
        data["salary_from"].append(None if "salary" not in posting or "from" not in posting["salary"]
                                   else posting["salary"]["from"])
        data["salary_to"].append(None if "salary" not in posting or "to" not in posting["salary"]
                                 else posting["salary"]["to"])
        data["employment_type"].append(None if "salary" not in posting or "type" not in posting["salary"]
                                       else posting["salary"]["type"])

    df = pandas.DataFrame.from_dict(data)
    file_path = f"out_test_{date.today()}.csv"
    df.to_csv(file_path, index=False, mode='a', header=not os.path.exists(file_path))
    return len(df)


def request_postings(index) -> any:
    res = requests.get(f"https://nofluffjobs.com/api/joboffers/main?pageTo={index}&pageSize=20&"
                       "withSalaryMatch=true&salaryCurrency=PLN&salaryPeriod=month&region=pl&language=pl-PL")
    return res.json()


def scrap_links(start=START_PAGE, amount=MAX_PAGE) -> None:
    for i in range(start, start + amount):
        json = request_postings(i)
        extracted_rows = extract_postings(json)
        print(f"EXTRACTED {i + 1}/{start + amount}: ROWS {extracted_rows}")
        if extracted_rows == 0:
            break
        time.sleep(3)
