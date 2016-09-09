# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Myriad - YARN Resource Manager
#
# VERSION 0.1.0

FROM centos:7
MAINTAINER Apache Myriad dev@myriad.incubator.apache.org

ENV YARN_USER="yarn"
ENV YARN_UID="107"
ENV HADOOP_GROUP="hadoop"
ENV HADOOP_GID="112"
ENV YARN_GROUP="yarn"
ENV YARN_GID="113"
ENV HADOOP_VER="2.7.0"
ENV HADOOP_HOME=/usr/lib/hadoop
ENV HADOOP_CONF_DIR=/usr/lib/hadoop/etc/hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.111-2.6.7.2.el7_2.x86_64
ENV YARN_HOME=/usr/lib/hadoop-yarn
ENV HADOOP_YARN_HOME=/usr/lib/hadoop-yarn
ENV YARN_CONF_DIR=$YARN_HOME/etc/hadoop
ENV MESOS_NATIVE_LIBRARY=/usr/local/lib/libmesos.so
ENV MESOS_VER=0.28.1
ENV HADOOP_LIBEXEC_DIR=/usr/lib/hadoop/libexec/

# Setup mesosphere repositories
RUN yum -y update
RUN yum install -y java-1.7.0-openjdk* wget sudo subversion cyrus-sasl-md5 lsb-release
RUN ln -s /usr/lib/jvm/java-1.7.0-openjdk-1.7.0.111-2.6.7.2.el7_2.x86_64 /usr/java

RUN sed -i 's/Defaults[ ]* requiretty/#Defaults      requiretty/g' /etc/sudoers
RUN echo "yarn ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
# Setup mesosphere repositories
#RUN rpm -Uvh http://repos.mesosphere.com/el/7/noarch/RPMS/mesosphere-el-repo-7-1.noarch.rpm

WORKDIR /tmp
RUN wget http://repos.mesosphere.com/el/7/x86_64/RPMS/mesos-0.28.1-2.0.20.centos701406.x86_64.rpm
RUN rpm -Uvh mesos-0.28.1-2.0.20.centos701406.x86_64.rpm
RUN yum -y update

# Install Mesos
#RUN yum install -y mesos=`apt-cache madison mesos | grep $MESOS_VER | head -1 | awk {'print $3'}` curl tar
RUN yum install -y mesos

# Download and install Cloudera 5
RUN wget http://archive.cloudera.com/cdh5/one-click-install/redhat/6/x86_64/cloudera-cdh-5-0.x86_64.rpm
RUN rpm -ivh cloudera-cdh-5-0.x86_64.rpm
RUN rpm --import https://archive.cloudera.com/cdh5/redhat/7/x86_64/cdh/RPM-GPG-KEY-cloudera
RUN yum -y install hadoop-mapreduce-historyserver hadoop-yarn-proxyserver hadoop-yarn-resourcemanager

# Cleanup
RUN rm -rf cloudera-cdh-5-0.x86_64.rpm mesosphere-el-repo-7-1.noarch.rpm 

# Copy over myriad libraries
ADD ./libs/* /usr/lib/hadoop-yarn/lib/

# Initialize hadoop confs with env vars
RUN echo "export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so" >> /usr/lib/hadoop-yarn/etc/hadoop/hadoop-env.sh
RUN chmod +x /usr/lib/hadoop-yarn/etc/hadoop/hadoop-env.sh   
RUN mkdir -p /var/lib/hadoop-yarn/cache/yarn/nm-local-dir
RUN mkdir -p /var/log/hadoop-yarn/containers
RUN chmod -R 777 /var
ADD myriad-etc/* /usr/lib/hadoop-yarn/etc/hadoop/
RUN chown -R root:root /usr/lib/hadoop-yarn/etc/hadoop
USER yarn
WORKDIR /usr/lib/hadoop-yarn
CMD bin/yarn resourcemanager
