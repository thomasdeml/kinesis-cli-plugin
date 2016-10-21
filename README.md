kinesis-cli-extension
=====================

# Kinesis AWS Command-line Interface Extension
The extension adds three commands to the AWS CLI
## 1. Push support 
   By piping output to the push extension, data gets put into the specified Kinesis Stream. 

   Example: `tail -f logfile | aws --region us-west-2 kinesis push --stream-name Test --partition-key test`
## 2. Pull support
   The pull command calls GetRecords in a loop for the specified stream and shard.

   Example: `aws --region us-west-2 kinesis pull --stream-name test --shard-id shardId-000000000001`
## 3. Shard-level metrics 
   Displays shard-level metrics for the specified stream. The command fetches the last 5 data points for all shards for the specified metric and statistic and displays them in order.

   Example: `aws --region us-west-2 kinesis get-shard-metrics --stream-name test --metric WriteProvisionedThroughputExceeded --statistic avg`

# Installation
## Install under Python site-packages:
`sudo pip install -U git+https://github.com/thomasdeml/kinesis-cli-extension.git --ignore-installed six`
## Install plugin in your ~/.aws/config file
`aws configure set plugins.kinesis kinesis_awscli_plugin

 
