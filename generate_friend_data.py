import argparse
from random import randint
import names
import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", dest="output", required=True)
    parser.add_argument("-n", "--nrows", dest="nrows", type=int)
    args = parser.parse_args()
    output_file = args.output
    n_rows = args.nrows or 100

    data = [{
        "name": names.get_full_name(),
        "age": randint(0, 100),
        "n_friends": randint(0, 300)
    } for _ in range(n_rows)]
    df = pd.DataFrame(data)
    df = df.reindex(columns=["name", "age", "n_friends"])
    df.to_csv(output_file)


if __name__ == "__main__":
    main()
