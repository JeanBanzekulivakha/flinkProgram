g# Flink Project 

### Application Requirements
- **Java version:** 11
- **Flink version:** 1.14.0

### Autors
* Zhan Banzekulivakha
* 

## Run the project

#### Download the data using python script
1. ```pip3 install requests ``` - install library `requests` that python3 script used
2. ``` python3 ./utils/download_data.py ``` - download data for app

#### Start Apache Flink cluster
1. Download [Apache Flink 1.14.0](https://www.apache.org/dyn/closer.lua/flink/flink-1.14.0/flink-1.14.0-bin-scala_2.12.tgz)
2. Unzip folder and run cmd: `./bin/start-cluster.sh`  

#### Compile and package the project
```
mvn clean package -Pbuild-jar
```

#### Run application
````
flink run -p 3 -c master.VehicleTelematics target/cloud-project-1.0-SNAPSHOT.jar src/main/resources/sample-traffic-3xways.csv src/main/resources/output.txt
````

You can see your app here: http://localhost:8081/#/overview

#### Stop cluster
````
./bin/stop-cluster.sh
````


### Problems
1. If you have problem with Java Version:
   1. Check your actual version of java: ```java --version```
   2. Check your installed java versions: ```/usr/libexec/java_home -V```
   3. Switch active JDK: ```export JAVA_HOME=`/usr/libexec/java_home -v <version>` ```
   Example:
   ```
   export JAVA_HOME=`/usr/libexec/java_home -v 11.0.13`
   ```
2. ...