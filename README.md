# Usage 

`java -jar kafka-to-influxdb.jar <runMode>`

`<runMode>` - **LOCAL** or **CLUSTER**, default is **LOCAL**

## Configuration

You need to have a Kafka & influxDB installed. You can modify `source.properties` 
and `sink.properties` under `resources` folder accordingly.

## Local Run

* Compile the code: `mvn package`

* Run the jar: `java -jar target/kafka-to-influxdb.jar`

This will create a 2 node Jet cluster embeded & run the job.


# Clustered Run

* Compile the code: `mvn package`

* Download Hazelcast Jet distribution to all servers: https://jet.hazelcast.org/download/

* Run each nodes using the start script in the distribution: `./jet-start.sh`

* Submit the job to the cluster using the submit scrpit: `./jet-submit.sh kafka-to-influxdb.jar CLUSTER`


