echo "copying source to /usr/local/aws/lib/python2.7/site-packages/awscli/customizations directory"
sudo rm -rf /usr/local/aws/lib/python2.7/site-packages/awscli/customizations/kinesis
sudo mkdir /usr/local/aws/lib/python2.7/site-packages/awscli/customizations/kinesis
sudo cp -r kinesis/*  /usr/local/aws/lib/python2.7/site-packages/awscli/customizations/kinesis/
