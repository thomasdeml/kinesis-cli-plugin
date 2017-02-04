Kinesis AWS Command-line Interface Plugin
=========================================
This Plugin adds four Kinesis commands to the AWS CLI

# Installation
   Use pip to install the Kinesis AWS CLI Plugin under Python site-packages:

   `sudo pip install https://s3.amazonaws.com/thomasdeml/kinesis_awscli_plugin-0.1.zip`

   On a Mac you might have to install the plugin like this:

   `sudo pip install https://s3.amazonaws.com/thomasdeml/kinesis_awscli_plugin-0.1.zip --ignore-installed six`

### AWS CLI Plugin Registration
   Execute the following command to register the plugin in your ~/.aws/config file

   `aws configure set plugins.kinesis kinesis_awscli_plugin`
  
   This will add the plugin to the ~/.aws/config file.

# Usage

### 1. Stream Metrics
   Displays stream-level metrics for the specified stream. By default datapoints of the last 10 minutes get fetched.  

   **Example 1:** 
   
   Fetches all metrics for the specified stream: 

   `aws kinesis get-stream-metrics --stream-name Test`
  
   **Example 2:** 

   Fetches all metrics for the specified stream and displays them in readable format: 

   `aws kinesis get-stream-metrics --stream-name Test --output table`



   More details with `aws kinesis get-stream-metrics help`.

### 2. Shard-level Metrics 
   Displays shard-level metrics for the specified stream. By default the datapoints of the last 10 minutes get fetched and averaged. The metric name used by default is 'IncomingRecords'.   

   **Example 1:** 
   
   Simple shard metrics query using the defaults (average of IncomingRecords over the last 15 minutes). Output is always sorted by average in descending order.

   `aws kinesis get-shard-metrics --stream-name Test`

   **Example 2:** 
   
   Shard metrics query using a different metric and statistic. Output is sorted by average of datapoints for the shards in descending order. 

   `aws kinesis get-shard-metrics --stream-name Test --metric-name WriteProvisionedThroughputExceeded --statistic Sum`

   **Example 3:** 
   
   Use other metric name and different start and end time. Time needs to be in UTC.

   `aws kinesis get-shard-metrics --stream-name Test --metric-name IncomingBytes --start-time 2016-10-10T10:10:00Z --end-time 2016-10-10T11:10:00Z`
   
   if the Python dateparser module is installed (`pip install dateparser`) you can also use relative dates like `--start-time "20 minutes ago"`
 
   **Example 4:**
   
   Show only the average of the datapoints over the queried timeframe. Output is also in table format.

   `aws kinesis get-shard-metrics --stream-name Test --metric-name IncomingBytes --statistic Sum --query "ShardMetrics[*].{ShardId:ShardId,DatapointAverage:DatapointAverage}" --output table`



   More details with `aws kinesis get-shard-metrics help`.

### 3. Push 
   By piping output to the push extension, data gets put into the specified Kinesis Stream. 

   **Example 1:** 
   
   Puts every new line of currently populated logfile into a record and calls PutRecord. The partition key is the MD5 hash of the payload. 

   `tail -f logfile | aws kinesis push --stream-name Test --disable-batch`
  
   **Example 2:** 

   Puts the content of every log file in the /var/log directory into Kinesis. Lines are batched into a single record until the record reaches 50kB. Partition key is the current host name.  

   `cat /var/log/*.log | aws kinesis push --stream-name Test --partition-key $(hostname)`



   More details with `aws kinesis push help`.

### 4. Pull
   The pull command calls GetRecords in a loop for the specified stream and shard.

   **Example 1:** 

   This command retrieves data from shard 0 of stream Test. GetRecords is called every 5000 ms (default). 

   `aws kinesis pull --stream-name Test --shard-id ShardId-000000000000`

   **Example 2:**

   This command retrieves data from shard 0 of stream Test. It calls GetRecords every 500ms. 
    
   `aws kinesis pull --stream-name Test --shard-id ShardId-00000000000 --pull-delay 500`


   
   More details with `aws kinesis pull help`.
