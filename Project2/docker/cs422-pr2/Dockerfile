FROM diasbase

# Install hdfs
RUN wget http://archive.apache.org/dist/hadoop/common/hadoop-2.7.5/hadoop-2.7.5.tar.gz
RUN tar -xvzf hadoop-2.7.5.tar.gz
RUN rm hadoop-2.7.5.tar.gz
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
ENV HADOOP_HOME=/root/hadoop-2.7.5

ADD config/masters $HADOOP_HOME/etc/hadoop/masters
ADD config/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
ADD config/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
ADD config/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml
ADD config/yarn-site.xml $HADOOP_HOME/etc/hadoop/yarn-site.xml

# Install Spark
RUN wget http://mirror.easyname.ch/apache/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz
RUN tar -xvzf spark-2.2.1-bin-hadoop2.7.tgz
RUN rm spark-2.2.1-bin-hadoop2.7.tgz
ENV SPARK_HOME=/root/spark-2.2.1-bin-hadoop2.7

# Connect spark with hdfs
RUN ln -s $HADOOP_HOME/etc/hadoop/core-site.xml $SPARK_HOME/conf/core-site.xml
RUN ln -s $HADOOP_HOME/etc/hadoop/hdfs-site.xml $SPARK_HOME/conf/hdfs-site.xml
