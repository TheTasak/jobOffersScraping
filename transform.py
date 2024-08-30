import pandas
import os


def load_file(file):
    df = pandas.read_csv(file, sep=",")
    df.value_counts("company", normalize=True).to_csv("test.csv")


def normalize_index(file):
    df = pandas.read_csv(file, sep=",")
    df = df.drop(columns=['description'])

    try:
        skills_df = pandas.read_csv(file.split("/")[0] + "/skills.csv", sep=",")
    except FileNotFoundError:
        skills_df = pandas.DataFrame(columns=['id', 'name'])
        i = 0
    else:
        i = skills_df["id"].max() + 1

    skills = {
        "id": [],
        "name": [],
    }
    for index, row in df.iterrows():
        skill_arr = [skill.strip().lower() for skill in row["skills"].split("\n")]
        for skill in skill_arr:
            found_rows = skills_df[skills_df["name"] == skill]
            if len(found_rows) == 0 and skill not in skills["name"]:
                skills["id"].append(i)
                skills["name"].append(skill)
                i += 1

    df.to_csv(file.split("/")[0] + "/test.csv", index=False, mode='w')

    new_skills = pandas.DataFrame.from_dict(skills)
    skills_path = file.split("/")[0] + "/skills.csv"
    new_skills.to_csv(skills_path, index=False, mode='a', header=not os.path.exists(skills_path))
