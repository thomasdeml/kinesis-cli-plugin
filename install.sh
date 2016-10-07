USR_LOCATION=/usr/local/aws/lib/python2.7/site-packages/awscli/customizations
LIB_LOCATION=/Library/Python/2.7/site-packages/awscli/customizations/

if [ -e $USR_LOCATION ] 
  then
    echo "copying source to $USR_LOCATION directory" 
    sudo cp -R kinesis  $USR_LOCATION 
fi

if [ -e $LIB_LOCATION ]
  then
    echo "Copying sources to $LIB_LOCATION directory"
    sudo cp -R kinesis $LIB_LOCATION
fi
