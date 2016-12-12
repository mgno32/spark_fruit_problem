from pyspark import SparkConf, SparkContext
APP_NAME = "My Spark fruit problem"

def main(sc):
    text = sc.textFile("textFile")
    def split_txt(t):
        fruit_name = []
        for line in t:
                tmp  = t.split('\n')
                name, fruits = tmp.split(':')
                for fruit in fruits.split(','):
                    fruit_name.append((fruit,name))    
        return fruit_name
    fruit_names = text.flatMap(split_txt)
    wc = fruit_names.map(lambda x : (x[0],x[1]))
    def count_name(a,b):
        sa = set(a.split(',')
        sb = set(b.split(',')
        return ','.join([str(u) for u in (sa|sb)])

    fruit_name = wc.reduceByKey(count_name)
    fruit_name.saveAsTextFile("out")

if __name__ == "__main__":
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)

    main(sc)




