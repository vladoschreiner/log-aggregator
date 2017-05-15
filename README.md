# log-aggregator

Searches for a service with slowest average response time in the streaming way. Demonstrates working with multiple 
windows in one streaming job. 

In first step, the average response time per service is computed (1 sec tumbling window). In second step, the slowest 
average response is selected and updated every second (3 sec sliding window, 1 sec sliding step).

The [LogAggregation](https://github.com/vladoschreiner/log-aggregator/blob/master/src/main/java/LogAggregation.java) contains textual description of underlying DAG.

