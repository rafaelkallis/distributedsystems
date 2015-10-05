#!/bin/bash

# This is an example test script.
# A similar script will be used to grade your submission.
# Please note that in the actual evaluation, we will use our own implementation of the server and client to grade your submission. Please read README.txt for more information.

# input parameters: jar file, server port, name of producer, input file, output file

# Start the server
java -cp $1 Assignment1.Server $2 &
echo $! > serverpid.txt

# Start Producer 1
java -cp $1 Assignment1.Producer localhost $2 $3 $4

# Start Listener and write output to the specified output file
java -cp $1 Assignment1.Listener localhost $2 > $5 &
echo $! > clientpid.txt

# Start Producer 2
java -cp $1 Assignment1.Producer localhost $2 client2 $4

# Wait for 5 seconds
sleep 5

# Stop the server and listener
kill -9 `cat clientpid.txt`
kill -9 `cat serverpid.txt`

sed -i '' 's/^[^:]*://' $5

rm serverpid.txt
rm clientpid.txt

result=`diff sample_output.txt $5 | wc -l`
if [ $result -eq 0 ]
then
  echo "Test PASSED"
else
  echo "Test FAILED"
fi
