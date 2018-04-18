FROM ubuntu:16.04
WORKDIR /root

#Install ssh server
RUN apt-get update && apt-get install -y --no-install-recommends openssh-server openjdk-8-jdk wget scala
RUN mkdir /var/run/sshd

# Create keys
ADD keys/fakekey /root/.ssh/id_rsa
ADD keys/fakekey.pub /root/.ssh/id_rsa.pub
ADD keys/config /root/.ssh/config
RUN cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys
ADD config/bashrc /root/.bashrc
RUN chmod 400 /root/.ssh/id_rsa

ENTRYPOINT service ssh restart && sleep infinity
