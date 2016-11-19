from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)


text_file = sc.textFile("/Users/gvsi/Developer/bdp/spark/data/assign10/dataSet10.txt")

def wordMapFunction(line):
    i = line.index(' ')
    verse = line[:i]
    content = re.sub(r'\.|,|--|;|:|\?|\"|=|_|!|\(|\)?', '', line[i+1:]).lower().split(" ")
    return map(lambda word : (word, [verse]), list(set(content)))


def comparator(o1, o2):
    parts1 = o1.split(":")
    parts2 = o2.split(":")
    book1 = parts1[0]
    book2 = parts2[0]
    chapter1 = int(parts1[1])
    chapter2 = int(parts2[1])
    verse1 = int(parts1[2])
    verse2 = int(parts2[2])
    if book1 > book2:
        return 1
    elif book1 < book2:
        return -1
    if chapter1 > chapter2:
        return 1
    elif chapter1 < chapter2:
        return -1
    if verse1 > verse2:
        return 1
    elif verse1 < verse2:
        return -1
    return 0

inverted_index = text_file.filter(lambda line: len(line) > 0).flatMap(wordMapFunction).reduceByKey(lambda a, b: a + b).map(lambda tuple: (tuple[0], sorted(tuple[1],comparator))).sortByKey()
inverted_index.saveAsTextFile("/Users/gvsi/Downloads/t")

# Extra credit
sorted_by_references = inverted_index.sortBy(lambda tuple: len(tuple[1]), False)
sorted_by_references.saveAsTextFile("/Users/gvsi/Downloads/t2")