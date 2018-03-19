import os
from pyspark import SparkContext, SparkConf


def parse_line(line: str):
    """
    Parse a line in Spark RDD
    :param line: a line in Spark RDD
    :type line: tuple
    :return: a tuple of age and n_friends
    :rtype: tuple
    """
    fields = line.split(",")
    age = int(fields[2])
    n_friends = int(fields[3])
    return age, n_friends


def main():
    app_name = os.path.basename(__file__).split(".")[0]
    conf = SparkConf() \
        .setMaster("local") \
        .setAppName(app_name)
    sc = SparkContext(conf=conf)
    sc_text_file_path = os.path.join(os.path.dirname(__file__), "data", "friends.csv")
    assert os.path.exists(sc_text_file_path), "{} doesn't exist".format(sc_text_file_path)
    lines = sc.textFile(sc_text_file_path)

    # Make header an RDD
    header = sc.parallelize([lines.first()])
    # Get rid of the header
    lines = lines.subtract(header)
    friends_rdd = lines.map(parse_line)
    totals_by_age = friends_rdd \
        .mapValues(lambda x: (x, 1)) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    average_by_age = totals_by_age.mapValues(lambda x: round(x[0] / x[1], 2))
    results = average_by_age.collect()  # type: list
    results.sort(key=lambda tup: tup[0])
    for result in results:
        print(result)


if __name__ == "__main__":
    main()



