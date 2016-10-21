kinesis-cli-extension
=====================

# Kinesis AWS Command-line Interface Extension
The extension adds three commands to the AWS CLI
## 1. Shard-level metrics 
   Displays shard-level metrics for the specified stream. By default the datapoints of the last 15 minutes get fetched and averaged. The metric name used by default is 'IncomingRecords'. Use  

   Example 1: `aws --region us-west-2 kinesis get-shard-metrics --stream-name test`

   Example 2: `aws --region us-west-2 kinesis get-shard-metrics --stream-name test --metric WriteProvisionedThroughputExceeded --statistic Sum`

   Example 3: `aws --region us-west-2 kinesis get-shard-metrics --stream-name test --metric WriteProvisionedThroughputExceeded --start-time 2016-10-10T10:10 --end-time 2016-10-10T11:10`

# Installation
## Install under Python site-packages:
`sudo pip install -U git+https://github.com/thomasdeml/kinesis-cli-extension.git --ignore-installed six`
## Install plugin in your ~/.aws/config file
`aws configure set plugins.kinesis kinesis_awscli_plugin`

# NOT QUITE READY
 ## 2. Push support 
   By piping output to the push extension, data gets put into the specified Kinesis Stream. 

   Example: `tail -f logfile | aws --region us-west-2 kinesis push --stream-name Test --partition-key test`
## 3. Pull support
   The pull command calls GetRecords in a loop for the specified stream and shard.

   Example: `aws --region us-west-2 kinesis pull --stream-name test --shard-id shardId-000000000001`

