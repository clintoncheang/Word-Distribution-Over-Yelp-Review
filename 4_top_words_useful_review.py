from pyspark import SparkConf, SparkContext
from pyspark import sql
from pyspark.sql import SQLContext
from pyspark.sql.functions import avg

# conf=SparkConf().setMaster('yarn-client').setAppName("t1")

conf=SparkConf().setAppName("t1") #####local
sc=SparkContext(conf=conf) 
sqlContext = sql.SQLContext(sc)

lines=sc.textFile("text_Final.txt") 
import string

import operator  
import nltk
import numpy as np 
from nltk.corpus import stopwords

nltk.download('stopwords') 
 

##############remove all the puctuation in the text and convert to lower case 
rdd1=lines.map(lambda x: (''.join([i for i in x if i not in string.punctuation] ))).map(lambda x: x.lower()) 
############## remove all the stop words and numbers 
stop = list(stopwords.words('english')) 
add_list = ['','1','2','3','4','5','6','7','8','9','0']
stop.extend(add_list)

rdd2 = rdd1.map(lambda x: [i for i in x.split(' ') if i not in stop])
############## rdd2 is a list of words in a list; take it from one list 
rdd3= rdd2.flatMap(lambda x:x)
ls = rdd3.collect()
############## remove all the stemming words 
# import nltk
# nltk.download('wordnet')
# from nltk.stem import WordNetLemmatizer
# wordnet_lemmatizer = WordNetLemmatizer()

# singles=[wordnet_lemmatizer.lemmatize(word) for word in ls ] 


from nltk.stem import *
stemmer=PorterStemmer() 
singles=[stemmer.stem(word) for word in ls ] 
# singles_2=[' '.join(singles)] 


############ save stingles to text file  


rdd_single = sc.parallelize(singles) 

############# Get word frequency 
from operator import add 

rdd4=rdd_single.map(lambda word: (word,1)).reduceByKey(add).sortBy(lambda x: x[1],ascending = False)

############ convert rdd4 to DF  
df_rev_use= rdd4.toDF(['word','count'])

df_rev_use.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("0430.csv")














