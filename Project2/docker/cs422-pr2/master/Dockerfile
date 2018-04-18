FROM diasslave

RUN $HADOOP_HOME/bin/hadoop namenode -format

ENTRYPOINT service ssh restart && $HADOOP_HOME/sbin/start-all.sh && scripts/$MASTER_SCRIPT
