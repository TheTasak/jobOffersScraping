import pandas


def load_file(file):
    df = pandas.read_csv(file, sep=",")
    df.value_counts("company", normalize=True).to_csv("test.csv")