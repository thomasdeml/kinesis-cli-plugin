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

   Example 1: 
   
   `aws kinesis get-shard-metrics --stream-name test`

   Example 2: 
   
   `aws kinesis get-shard-metrics --stream-name test --metric-name WriteProvisionedThroughputExceeded --statistic Sum`

   Example 3: 
   
   `aws kinesis get-shard-metrics --stream-name test --metric-name IncomingBytes --start-time 2016-10-10T10:10:00 --end-time 2016-10-10T11:10:00`

### 2. Push support 
   By piping output to the push extension, data gets put into the specified Kinesis Stream. 

   Example: `tail -f logfile | aws --region us-west-2 kinesis push --stream-name Test --partition-key test`

### 3. Pull support
   The pull command calls GetRecords in a loop for the specified stream and shard.

   Example: `aws --region us-west-2 kinesis pull --stream-name test --shard-id shardId-000000000001`

