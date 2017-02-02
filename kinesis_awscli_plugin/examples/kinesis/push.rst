

The push extensions is typically used by used by piping output to it. 

``Example 1:``

Puts every line of logfile into a record and calls PutRecord. The partition key is the MD5 hash of the payload.

tail -f logfile | aws kinesis push --stream-name Test --disable-batch

``Example 2:``

Puts the content of every log file in the /var/log directory into Kinesis. Lines are batched into a single record until the record reaches 50kB. Partition key is the current host name.

cat /var/log/* | aws kinesis push --stream-name Test --partition-key $(hostname)
