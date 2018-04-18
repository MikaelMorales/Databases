In this project you will use [Docker](https://www.docker.com/) to run a Hadoop and Spark cluster of containers.
Here we describe how can you start a cluster of 2 containers and how you can scale it based on your machine capabilities.

If you haven't used or heard of Docker before, don't get demotivated. Docker is one of the fastest-growing opensource projects with a strong and supportive community.
For any question [this](https://docs.docker.com/) is a good starting point.

# Step -1
Before doing anything you need to install docker on your machine.
Depending on your platform you can follow the instructions [here](https://docs.docker.com/install/)

# Step 0
We will need two different images, one for the master node and one for the slave nodes.
To build the images:

```
make build
```

# Step 1
To create and manage our cluster we will use [docker-compose](https://docs.docker.com/compose/).
Docker compose is a tool that enables describing and running multi-container applications.
Depending on the operating system on your machine you might need to install docker-compose separately.
You can see how to do that [here](https://docs.docker.com/compose/install/).

docker-compose uses .yml files to describe multi-container applications.
You can see a sample .yml file in `cs422-pr2/docker-compose.yml.`

For your ease, we create two Makefile directives to start and stop the cluster.

You can start the cluster with:
```
docker-compose -f cs422-pr2/docker-compose.yml up
```

Wait till you get the green light that you cluster is ready.

You can stop the cluster with `Ctrl+C` and restart it with `make cluster-up`.

To delete the cluster and start over run:
```
make cluster-down
```

## Shared directories
Every container shares the following directories with the host:

1. jars: You can find/put all the necessary jars there.
2. scripts: Includes any script that you might need to configure the containers or run jobs
3. workloads: Includes sample workload files to test your code.


## Port forwarding
To be able to check your cluster status we forwar the following master node ports to your local machine:

* 50070 (Hadoop)
* 8080 (Spark)

You can access them by entering `localhost:<port_no>` in your browser.

# Step 1.5
At this moment you should have a two-container cluster running. Spend some time understanding what you've already achieved and explore the cluster.
Here are two useful commands:

```
docker ps # You can see all the running containers
docker exec -it <container_hash> bash # you can get a terminal within a running container
```

If you run docker ps you'll see 2 containers running, one master and one slave.
Try entering master, exlore the HDFS contents, check the running services etc.

```
$HADOOP_HOME/bin/hadoop fs -ls / # Explore the / directory in HDFS
```

# Step 2
At this point you are ready to run your first Spark job in your cluster.
We've provided a sample jar with a Spark job that reads and writes to HDFS.
Also, we've added a script to run this job in `cs422-pr2/scripts/run-job.sh`.
This script can be accessed both within the container and from your local machine.
So, you can edit it from you favourite editor.

To run a Spark job:

1. Copy paste your jars from `target/scala-2.11` to `docker/cs422-pr2/jars`. (There is no need to do so for the sample job)
1. Edit `run-job.sh` accordingly. (It's already set for the sample job)
2. Enter the master node with `docker exec` as before
3. Within the master node run `/root/scripts/run-job.sh`

Note: If the job is killed make sure docker is configured with enough memory and increase it if necessary.

You can check the job results by running:

```
$HADOOP_HOME/bin/hadoop fs -ls /sample-results
```

within the master container.

# Scaling slaves;
You can add more slaves by editing the `cs422-pr2/config/slaves` file accordingly.
For example, for one extra slave you should add an extra line with "cs422pr2\_slave\_2" and then run:

```
docker-compose -f cs422-pr2/docker-compose.yml up --scale slave=2
```
