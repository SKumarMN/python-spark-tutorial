from pyspark import SparkConf,SparkContext


if __name__ == "__main__":
    print("hipy")
    conf= SparkConf().setAppName("word count").setMaster("local[*]")
    sc= SparkContext(conf= conf) 
    lines = sc.textFile("in/word_count.text")

    words = lines.flatMap(lambda line: line.split(" "))
    wordCounts = words.countByValue()
    for word,count in wordCounts.items():
        print("{}:{}".format(word,count))