Kinesis AWS Command-line Interface Plugin
=========================================
This Plugin adds three Kinesis commands to the AWS CLI

# Installation
### Pip Install 
   Execute the following command To install the Kinesis AWS CLI Plugin under Python site-packages:

   `sudo pip install -U git+https://github.com/thomasdeml/kinesis-cli-plugin.git --ignore-installed six`
### Plugin Registration
   Execute the following command to install the plugin in your ~/.aws/config file

   `aws configure set plugins.kinesis kinesis_awscli_plugin`

# Usage

### 1. Shard-level metrics 
   Displays shard-level metrics for the specified stream. By default the datapoints of the last 15 minutes get fetched and averaged. The metric name used by default is 'IncomingRecords'.   

   **Example 1:** 
   
   Simple shard metrics query using the defaults (average of IncomingRecords over the last 15 minutes). Output is always sorted by average in descending order.

   `aws kinesis get-shard-metrics --stream-name Test`

   **Example 2:** 
   
   Shard metrics query using some another metric and statistic. Output is sorted by average of datapoints for the shards in descending order. 

   `aws kinesis get-shard-metrics --stream-name Test --metric-name WriteProvisionedThroughputExceeded --statistic Sum`

   **Example 3:** 
   
   Use other metric name and different start and end time.

   `aws kinesis get-shard-metrics --stream-name Test --metric-name IncomingBytes --start-time 2016-10-10T10:10:00 --end-time 2016-10-10T11:10:00`
 
   **Example 4:**
   
   Show only the average of the datapoints over the queried timeframe. Output is also in table format.

   `aws kinesis get-shard-metrics --stream-name Test --metric-name IncomingBytes --statistic Sum --query "shard_metrics[*].{ShardId:ShardId,Average:Average}" --output table`

### 2. Push support 
   By piping output to the push extension, data gets put into the specified Kinesis Stream. 

   **Example 1:** 
   
   `tail -f logfile | aws kinesis push --stream-name Test --disable-batch`
  
   **Example 2:** 

   `cat /var/log/* | aws kinesis push --stream-name Test --partition-key $(hostname)`

### 3. Pull support
   The pull command calls GetRecords in a loop for the specified stream and shard.

   **Example 1:** 

   `aws kinesis pull --stream-name Test --shard-id shardId-000000000001`

   **Example 2:**

   `aws kinesis pull --stream-name Test --shard-id shardId-00000000000 --pull-delay 500`
