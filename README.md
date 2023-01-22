# big-data-projects
projects using hadoops and sparks map reduce jobs

For simplicity the data was generated, using [this code](https://github.com/tobias-mack/generate-dataset).

---

## DOCKER

1. start containers in detachable mode (background)
    - docker-compose up -d
  
2. get inside docker container
    - docker exec -it namenode /bin/bash
  
3. create dir in hdfs
	- hdfs dfs -mkdir -p /foldername  
  
4. lookup ip of namenode with ifconfig

5. lookup port of namenode with docker container -ls

6. copy jar to namenode
	- docker cp /pathToJar namenode:/tmp

	
---
## HADOOP over HDFS

Go to hadoop folder.

1. Format the filesystem:
	- bin/hdfs namenode -format

2. Start NameNode daemon and DataNode daemon:
  -	sbin/start-dfs.sh

3. create dir:
	- hdfs dfs -mkdir /foldername  

4. upload file
	- hdfs dfs -put fullPath/data.txt /foldername/
	
5. delete file
    - bin/hdfs dfs -rm -r /foldername/data.txt

---
run command:

path/hadoop jar path/project-0.jar WordCount /cs585/data.txt /cs585/output2.txt

e.g.
bin/hadoop jar /home/twobeers/IdeaProjects/wordCount/out/artifacts/wordCount_jar/wordCount.jar /cs585/data.txt /cs585/output2.txt

query1 bin/hadoop jar /home/twobeers/Desktop/bigData/big-data-projects/out/artifacts/big_data_projects_jar/big-data-projects.jar /project1/customers.csv /project1/output-query1.txt
