# This requires a Yarn setup...
virtualenv env_1 -p /usr/local/bin/python3  # create virtual environment env_1 
source env_1/bin/activate  # activate virtualenv

pip3 freeze > requirements.txt # find all install packages and put into requirements.txt for virtual env

spark-submit --master yarn-client\
	--conf spark.pyspark.virtualenv.enabled=true  \
	--conf spark.pyspark.virtualenv.type=native \
	--conf spark.pyspark.virtualenv.requirements=~/Scripts/requirements.txt \
	--conf spark.pyspark.virtualenv.bin.path=/Users/jzhang/anaconda/bin/virtualenv \
	--conf spark.pyspark.python=/usr/local/bin/python3 \
	spark_virtualenv.py