# Flink Project 

### Application Requirements
- **Java version:** 11
- **Flink version:** 1.14.0

### Autors
* Zhan Banzekulivakha
* 

##Run the project
*Download the data using python script*
1. ```pip3 install requests ``` - install library `requests` that python3 script used
2. ``` python3 ./utils/download_data.py ``` - download data for app

*Compile the project*
```
mvn clean package
```
*Invoke the job*
````
#Map invokation
./flink run ...
````