from pyspark import SparkContext
import re

if __name__ == "__main__":

    dir_path = "F:\Big_data_project\clog_temp"

    sc = SparkContext("local")

    # used wholeTextFile to fetch all the (filesnames -> file_data)
    counts = sc.wholeTextFiles(dir_path)

    #extracting industry names from the first parameter return by wholeTextFiles
    # 1. Remove the prefix of the file name by replacing the front dir path from
    # it and then take 4th element by spliting through dots
    # 2. Convert every word to lowercase
    # 3 Fetch distinct industry names and return the list
    industry_names = counts \
        .flatMap(lambda x: [(x[0].replace(dir_path, "").lower().split(".")[3])]) \
        .distinct()
    industry_names_map = sc.broadcast(industry_names.collect())


    #taking the whole output in one string for easy processing
    def format_text(string):
        one_line_text = string.replace('\r\n', " ")
        return one_line_text


    #checking industry name exists in post or not
    def check_industry_name(x):
        map = []
        for elems1 in x[1][0].split(" "):
            # formatted_elems = re.findall(r'"[A-Za-z-]+"', elems)
            # formatted_string = formatted_elem.lower().replace(".","")
            elems1 = elems1.strip('.')
            if elems1.lower() in industry_names_map.value:
                date_format = x[0][0].split(",")
                map.append(((elems1.lower(), (date_format[2] + '-' + date_format[1])), (1)))

        return map

    #fetching post and checking industry name as per the following logic
    # 1. Convert input in one string
    # 2. Fetch <date> to </post> blocks out of it
    # 3. Make ((industry,date)->post) key value pair
    # 4. Reduce them by key so that all the entries with same industry and same date added up
    # 5. Convert the map to (industry -> (date, count)) key value pair
    # 6. Apply reduce by key to merge all the arrays of same industry i.e (industry -> [date1, count]) to (industry -> [(date1, count), (date2, count)] format

    date_post = counts.map(lambda x: format_text(x[1])) \
        .flatMap(lambda x: re.findall(r'<date>.*?</post>', x)) \
        .map(lambda x: ((re.findall(r'<date>(.*?)</date>', x), re.findall(r'<post>(.*?)</post>', x)))) \
        .flatMap(lambda x: check_industry_name(x)) \
        .reduceByKey(lambda x, y: x + y) \
        .map(lambda x: (x[0][0], [(x[0][1], x[1])])) \
        .reduceByKey(lambda x, y: x + y)

    date_post_map = date_post.collect()

    for v in date_post_map:
        print(v)
