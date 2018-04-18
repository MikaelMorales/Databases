#/bin/sh

chmod 600 ~/.ssh/config
eval $(ssh-agent)
ssh-add

set -e

echo "Restart ssh..."
service ssh restart

echo "Start hadoop slaves..."
$HADOOP_HOME/sbin/start-all.sh

if ! $HADOOP_HOME/bin/hadoop fs -test -e /project2; then
  ## Add files to hdfs if necessary ##
  echo "Adding files to hdfs..."
  $HADOOP_HOME/bin/hadoop fs -mkdir /project2
  $HADOOP_HOME/bin/hadoop fs -put workloads/project2-input/* /project2

  $HADOOP_HOME/bin/hadoop fs -mkdir /samples
  $HADOOP_HOME/bin/hadoop fs -put workloads/samples/* /samples

  $HADOOP_HOME/bin/hadoop fs -mkdir /stream_input
fi

echo "Starting spark master..."
/root/spark-2.2.1-bin-hadoop2.7/sbin/start-master.sh

echo "Starting spark slaves"
for i in `cat $HADOOP_HOME/etc/hadoop/slaves`; do
  ssh $i /root/spark-2.2.1-bin-hadoop2.7/sbin/start-slave.sh spark://master:7077
done

GREEN='\033[0;32m'
NC='\033[0m'
echo "${GREEN}##### You can run your job now ####${NC}"

sleep infinity
