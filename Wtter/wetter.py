from time import strftime, time
from datetime import time, datetime
import time
import requests

from pyspark.sql import SparkSession
from wetter_variable import url_wetter
from pyspark.sql.types import StructType, StructField, StringType

spark = (
          SparkSession
                .builder
                .appName("Yello_test")
                .master("local[*]")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
                .getOrCreate()

)

class Bamberg_wetter:
      temperature = ""
      timestamp = ""
      ville = ""
      data = [{}]



def get_wetter(name):

            response = requests.get(url_wetter)
            if response.status_code == 200:
                write_to_html_file(response.text,name)
            else:
                  raise Exception("api server")


def write_to_html_file(val,name):
      try:
            wf = open("wetter.html", "w", encoding="utf-8")
            wf.write(val)
            wf.close()
            read_file_html("wetter.html",name)
      except IOError as io:
             print("ioerror")

def read_file_html(file,name):
      for line in open("wetter.html", "r",encoding='utf-8'):
           try:
                  if "0.0" in line:
                        Bamberg_wetter.timestamp = strftime("%y-%m-%d %H:%M:%S", time.localtime())
                        Bamberg_wetter.temperature = line.strip() # Utiliser strip() pour enlever les espaces ou sauts de ligne
                  else:
                        if name in line:
                              index = line.index(name)
                              Bamberg_wetter.ville = name
           except Exception as e:
                 print(e.__str__())


      Bamberg_wetter.data.append({"ville":Bamberg_wetter.ville,"temperature":Bamberg_wetter.temperature,"datetime":Bamberg_wetter.timestamp})


def write_to_db(df):
      pass
schema = StructType([
    StructField("ville", StringType(), True),
    StructField("temperature", StringType(), True),
    StructField("datetime", StringType(), True)
])
def show_df(data,sch):
    df = spark.createDataFrame(data,sch)
    df.show()

try:
    while True:
      get_wetter("bamberg")
      show_df(Bamberg_wetter.data,schema)
      time.sleep(60)
except Exception as e :
      print(e.__str__())




