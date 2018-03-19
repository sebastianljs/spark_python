import os
import collections
from pyspark import SparkConf, SparkContext


def main():
    conf = SparkConf() \
            .setMaster("local") \
            .setAppName("UserOccupationHistogram")
    sc = SparkContext(conf=conf)
    sc_text_file_path = os.path.join(
        os.path.dirname(__file__), "ml-100k", "u.user")
    assert os.path.exists(sc_text_file_path), "{} doesn't exist".format(sc_text_file_path)
    lines = sc.textFile(sc_text_file_path)
    occupations = lines.map(lambda x: x.split("|")[3])
    result = occupations.countByValue()  # type: dict
    sorted_results = collections.OrderedDict(sorted(result.items()))

    for key, value in sorted_results.items():
        print("{} : {}".format(key, value))


if __name__ == "__main__":
    main()
