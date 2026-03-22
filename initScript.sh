pip3 install pyspark
pip3 install pyiceberg
pip3 install faker

# Clean up
rm *.jar
rm -rf warehouse
mkdir warehouse

# Init
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.0/iceberg-spark-runtime-4.0_2.13-1.10.0.jar
brew reinstall apache-spark

