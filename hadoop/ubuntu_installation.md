# 우분투에서 YARN 클러스터 설정 후 예제 프로그램 실행

- 우분투는 AWS 우분투 20.04
- 의사 분산(Pseudo Distributed) 모드로 YARN 클러스터 설정하기
- WordCount MapReduce 프로그램 실행해보기

## 자바 설치하기

```
java -version
sudo apt update
sudo apt install openjdk-8-jre-headless
sudo apt install openjdk-8-jdk-headless
java -version
```

## YARN 클러스터 설정하기

### 먼저 YARN 클러스터 전용 계정 만들고 그 계정으로 스위치

```
sudo adduser hdoop
su - hdoop
```

### hdoop 계정이 패스워드 없이 로그인이 가능하도록 조치

- SSH key pair를 만들고 authroized_keys 파일에 등록
- ssh localhost로 테스트

```
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
chmod 0600 ~/.ssh/authorized_keys
ssh localhost
```

### 하둡 3.3.4를 다운로드하고 압축 해제

- 압축하고 현재 디렉토리 확인 (/home/hdoop이어야 함)
- 하둡이 설치된 디렉토리는 "/home/hdoop/hadoop-3.3.4"이어야 함

```
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz
tar xzf hadoop-3.3.4.tar.gz
pwd
```

### ~/.bashrc에 다양한 환경 변수 등록

- vi등의 에디터를 사용해 ~/.bashrc에 다음 환경 변수들을 등록

```
#Hadoop Related Options
export HADOOP_HOME=/home/hdoop/hadoop-3.3.4
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
```

- 변경된 .bashrc 내용을 반영하려면 아래 명령을 실행

```
source ~/.bashrc
```

### 이제부터 5개의 환경설정 파일들을 수정

- hadoop-env.sh (마스터 환경 설정 파일 - JAVA_HOME 환경변수 등록)
- core-site.xml (HDFS 내임노드 설정)
- hdfs-site.xml (HDFS 관련 데이터가 저장되는 디렉토리 설정)
- mapred-site.xml (MapReduce 관련 환경 설정)
- yarn-site.xml (YARN 관련 환경 설정)

#### $HADOOP_HOME/etc/hadoop/hadoop-env.sh 파일 수정

- 하둡은 JAVA 8 이상을 필요로 하며 이는 AWS 우분투 20.04에는 기본으로 설치됨.
- 자바 버전부터 확인 (javac)
```
which javac
readlink -f /usr/bin/javac
```

- JAVA_HOME 환경변수를 hadoop-env.sh에 등록
```
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
```
   
#### $HADOOP_HOME/etc/hadoop/core-site.xml 파일 수정

- <configuration>과 </configuration> 사이의 내용을 아래처럼 수정하고 저장
- 내임노드 관련 설정이 여기에 이뤄짐 (포트 번호 9000)

```
<configuration>
<property>
  <name>hadoop.tmp.dir</name>
  <value>/home/hdoop/tmpdata</value>
</property>
<property>
  <name>fs.default.name</name>
  <value>hdfs://127.0.0.1:9000</value>
</property>
</configuration>
```

#### $HADOOP_HOME/etc/hadoop/hdfs-site.xml 파일 수정

- <configuration>과 </configuration> 사이의 내용을 아래처럼 수정하고 저장
- HDFS 관련 데이터가 저장되는 디렉토리 설정 (dfs.data.dir, dfs.replication)

```
<configuration>
<property>
  <name>dfs.data.dir</name>
  <value>/home/hdoop/dfsdata/namenode</value>
</property>
<property>
  <name>dfs.data.dir</name>
  <value>/home/hdoop/dfsdata/datanode</value>
</property>
<property>
  <name>dfs.replication</name>
  <value>1</value>
</property>
</configuration>
```

#### $HADOOP_HOME/etc/hadoop/mapred-site.xml 파일 수정

- <configuration>과 </configuration> 사이의 내용을 아래처럼 수정하고 저장
- MapReduce가 동작할 프레임웍을 YARN으로 지정

```
<configuration>
<property> 
  <name>mapreduce.framework.name</name> 
  <value>yarn</value> 
</property> 
</configuration>
```

#### $HADOOP_HOME/etc/hadoop/yarn-site.xml 파일 수정

- <configuration>과 </configuration> 사이의 내용을 아래처럼 수정하고 저장
- YARN과 관련된 Node Manager와 Resource Manager 관련 정보를 설정

```
<configuration>
<property>
  <name>yarn.nodemanager.aux-services</name>
  <value>mapreduce_shuffle</value>
</property>
<property>
  <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
  <value>org.apache.hadoop.mapred.ShuffleHandler</value>
</property>
<property>
  <name>yarn.resourcemanager.hostname</name>
  <value>localhost</value>
</property>
<property>
  <name>yarn.acl.enable</name>
  <value>0</value>
</property>
<property>
  <name>yarn.nodemanager.env-whitelist</name>   
  <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PERPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_MAPRED_HOME</value>
</property>
</configuration>
```

### HDFS를 포맷

```
hdfs namenode -format
```

### HDFS와 YARN을 실행

```
cd hadoop-3.3.4/sbin/
./start-dfs.sh
./start-yarn.sh
```

### 하둡 데몬들이 제대로 실행되었는지 확인

- jps를 실행하면 아래처럼 5개의 데몬이 실행되어 있는 것을 확인할 수 있어야함

```
hdoop@ip-172-31-xxx:~/hadoop-3.3.4$ jps
25347 NodeManager
2122 DataNode
29819 Jps
25195 ResourceManager
2364 SecondaryNameNode
1965 NameNode
```

### 하둡 관련 웹 UI를 브라우저에서 실행

하둡이 설치된 호스트 이름이 host라고 가정하면 해당 포트가 오픈이 되어있다면 가정:

- 내임노드: http://host:9870/
- 데이터노드: http://host:9864/
- 리소스 매니저: http://host:8088/


## WordCount MapReduce 프로그램 실행해보기

### HDFS 관련 디렉토리를 생성

- hdfs 커맨드를 사용해 /user과 /user/hdoop 디렉토리 생성
- hdfs 커맨드를 사용해 뒤에 WordCount 프로그램에서 사용할 입력 파일이 저장되는 input 디렉토리 생성

```
cd ~/hadoop-3.3.4
bin/hdfs dfs -mkdir /user
bin/hdfs dfs -mkdir /user/hdoop
bin/hdfs dfs -mkdir input
```

### 입력 파일 준비

- words.txt 파일을 오픈해서 아무 내용이 타입해서 저장. 예를 들면 아래와 같은 내용으로 구성

```
the brave yellow lion the lion ate the cow now the lion is happy
```

- 이를 앞서 만든 input 디렉토리로 업로드

```
bin/hdfs dfs -put words.txt input
```

제대로 업로드되었는지 확인 

```
bin/hdfs dfs -ls input
```

### 단어세기 프로그램을 실행

- output 디렉토리에 결과를 저장할 예정인데 만일 앞서 실행되어 디렉토리가 존재한다면 잡이 실패함. 먼저 그 디렉토리를 삭제

```
bin/hdfs dfs -rm -f -r output
```

- wordcount 프로그램은 하둡에 디폴트로 제공됨 (share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.4.jar)

```
bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.4.jar wordcount input output
```

예를 들면 아래와 같은 출력이 화면에 보여야 함

```
2023-01-09 08:22:34,423 INFO client.DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at ec2-44-195-xxx.compute-1.amazonaws.com/172.31.8.17:8032
2023-01-09 08:22:34,924 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/hdoop/.staging/job_1673227869835_0001
2023-01-09 08:22:35,225 INFO input.FileInputFormat: Total input files to process : 1
2023-01-09 08:22:35,288 INFO mapreduce.JobSubmitter: number of splits:1
2023-01-09 08:22:35,916 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1673227869835_0001
2023-01-09 08:22:35,917 INFO mapreduce.JobSubmitter: Executing with tokens: []
2023-01-09 08:22:36,147 INFO conf.Configuration: resource-types.xml not found
2023-01-09 08:22:36,148 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2023-01-09 08:22:36,578 INFO impl.YarnClientImpl: Submitted application application_1673227869835_0001
2023-01-09 08:22:36,622 INFO mapreduce.Job: The url to track the job: http://ec2-44-195-xxx.compute-1.amazonaws.com:8088/proxy/application_1673227869835_0001/
2023-01-09 08:22:36,622 INFO mapreduce.Job: Running job: job_1673227869835_0001
2023-01-09 08:22:45,781 INFO mapreduce.Job: Job job_1673227869835_0001 running in uber mode : false
2023-01-09 08:22:45,783 INFO mapreduce.Job:  map 0% reduce 0%
2023-01-09 08:22:50,848 INFO mapreduce.Job:  map 100% reduce 0%
2023-01-09 08:22:56,892 INFO mapreduce.Job:  map 100% reduce 100%
2023-01-09 08:22:57,908 INFO mapreduce.Job: Job job_1673227869835_0001 completed successfully
2023-01-09 08:22:58,029 INFO mapreduce.Job: Counters: 54
	File System Counters
		FILE: Number of bytes read=103
		FILE: Number of bytes written=551575
		FILE: Number of read operations=0
		FILE: Number of large read operations=0
		FILE: Number of write operations=0
		HDFS: Number of bytes read=178
		HDFS: Number of bytes written=61
		HDFS: Number of read operations=8
		HDFS: Number of large read operations=0
		HDFS: Number of write operations=2
		HDFS: Number of bytes read erasure-coded=0
	Job Counters 
		Launched map tasks=1
		Launched reduce tasks=1
		Data-local map tasks=1
		Total time spent by all maps in occupied slots (ms)=2837
		Total time spent by all reduces in occupied slots (ms)=3008
		Total time spent by all map tasks (ms)=2837
		Total time spent by all reduce tasks (ms)=3008
		Total vcore-milliseconds taken by all map tasks=2837
		Total vcore-milliseconds taken by all reduce tasks=3008
		Total megabyte-milliseconds taken by all map tasks=2905088
		Total megabyte-milliseconds taken by all reduce tasks=3080192
	Map-Reduce Framework
		Map input records=1
		Map output records=14
		Map output bytes=121
		Map output materialized bytes=103
		Input split bytes=113
		Combine input records=14
		Combine output records=9
		Reduce input groups=9
		Reduce shuffle bytes=103
		Reduce input records=9
		Reduce output records=9
		Spilled Records=18
		Shuffled Maps =1
		Failed Shuffles=0
		Merged Map outputs=1
		GC time elapsed (ms)=162
		CPU time spent (ms)=1150
		Physical memory (bytes) snapshot=475684864
		Virtual memory (bytes) snapshot=5063700480
		Total committed heap usage (bytes)=398983168
		Peak Map Physical memory (bytes)=283598848
		Peak Map Virtual memory (bytes)=2529026048
		Peak Reduce Physical memory (bytes)=192086016
		Peak Reduce Virtual memory (bytes)=2534674432
	Shuffle Errors
		BAD_ID=0
		CONNECTION=0
		IO_ERROR=0
		WRONG_LENGTH=0
		WRONG_MAP=0
		WRONG_REDUCE=0
	File Input Format Counters 
		Bytes Read=65
	File Output Format Counters 
		Bytes Written=61
```

#### 다음으로 결과 디렉토리를 확인해보고 결과 파일을 출력

```
bin/hdfs dfs -ls output
Found 2 items
-rw-r--r--   1 hdoop supergroup          0 2023-01-09 08:22 output/_SUCCESS
-rw-r--r--   1 hdoop supergroup         61 2023-01-09 08:22 output/part-r-00000
```

```
bin/hdfs dfs -cat output/part-r-00000
ate	1
brave	1
cow	1
happy	1
is	1
lion	3
now	1
the	4
yellow	1
```
