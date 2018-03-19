import os
from pyspark import SparkConf, SparkContext


def parse_line(line: str) -> tuple:
    """
    Parse a line of Spark RDD
    :param line:
    :return:
    """
    fields = line.split(",")
    station_id = fields[0]
    entry_type = fields[2]
    # Convert to Fahrenheit
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return station_id, entry_type, round(temperature, 3)


def main():
    """
    Runs the script
    :return:
    """
    app_name = os.path.basename(__file__).split(".")[0]
    conf = SparkConf() \
        .setMaster("local") \
        .setAppName(app_name)
    sc = SparkContext(conf=conf)
    sc_text_file_path = os.path.join(
        os.path.dirname(__file__), "data", "weather.csv")
    assert os.path.exists(sc_text_file_path), "{} doesn't exist".format(sc_text_file_path)
    lines = sc.textFile(sc_text_file_path)
    parsed_lines = lines.map(parse_line)

    # Find min temps
    min_temps = parsed_lines.filter(lambda x: "TMIN" in x[1])
    min_temps_by_station = min_temps.map(lambda x: (x[0], x[2]))
    absolute_min_temps_by_station = min_temps_by_station.reduceByKey(lambda x, y: min(x, y))
    min_temp_results = absolute_min_temps_by_station.collect()
    min_temp_results.sort(key=lambda tup: tup[0])
    for result in min_temp_results:
        print(result)

    # Find max temps
    max_temps = parsed_lines.filter(lambda x: "TMAX" in x[1])
    max_temp_by_station = max_temps.map(lambda x: (x[0], x[2]))
    absolute_max_temps_by_station = max_temp_by_station.reduceByKey(lambda x, y: max(x, y))
    max_temp_results = absolute_max_temps_by_station.collect()
    max_temp_results.sort(key=lambda tup: tup[0])
    for result in max_temp_results:
        print(result)


if __name__ == "__main__":
    main()
