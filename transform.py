import csv

import pandas
import os
from io import StringIO
from sqlalchemy import create_engine, text, Connection
from dotenv import find_dotenv, get_key

FILE_TABLE = 'occurrences'
INDEX_TABLE = 'jobs'


def check_if_column_exists(conn: Connection, table: str, column: str) -> bool:
    query = ("SELECT EXISTS (SELECT 1 FROM information_schema.columns "
             "WHERE table_name='{}' and column_name='{}');").format(table, column)
    result = conn.execute(text(query))
    res = result.fetchone()
    return res[0]


def search_term(term: str, lang: str = 'en', conn: Connection = None):
    close = False

    if conn is None:
        close = True
        engine = create_conn()
        conn = engine.connect()

    query = ("SELECT url, ts_rank_cd(description_search_{}, query) AS rank FROM jobs, websearch_to_tsquery('{}') query "
             "WHERE query @@ description_search_{} ORDER BY rank DESC;").format(lang, term, lang)
    result = conn.execute(text(query))

    if close:
        conn.close()

    return result.fetchall()


def psql_insert_copy(table, engine, keys, data_iter):
    conn = engine.connection
    with conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def create_conn():
    env = find_dotenv()
    username = get_key(env, key_to_get="USERNAME")
    db = get_key(env, key_to_get="DATABASE")
    return create_engine(f'postgresql+psycopg2://{username}:@localhost:5432/{db}')


def load_file_to_db(file: str) -> bool:
    engine = create_conn()

    if not os.path.exists(file):
        return False

    df = pandas.read_csv(file, sep=",")
    df["source"] = file.split("/")[0]
    df.to_sql(FILE_TABLE, engine, if_exists='append', method=psql_insert_copy, index=False)

    return True


def load_iterate_index_to_db(file: str) -> bool:
    engine = create_conn()

    if not os.path.exists(file):
        return False

    df = pandas.read_csv(file, sep=",")
    df["source"] = file.split("/")[0]

    conn = engine.connect()

    result = conn.execute(text(f"SELECT max(id) FROM jobs WHERE source='{file.split("/")[0]}'"))
    max_id = result.fetchone()[0]
    if max_id is None:
        max_id = -1
    df = df[df["id"] > max_id]

    df.to_sql(INDEX_TABLE, engine, if_exists='append', method=psql_insert_copy, index=False)
    conn.close()
    return True


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
