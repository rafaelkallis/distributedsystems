#!/bin/bash
# Bibek Paudel, Univerzity of Zurich

#arguments to this shell script
#1. jar file (e.g., myjarfile.jar)
#2. path prefix (e.g., /user/hue/)

#How to run this script:
#use a UNIX environment with Hadoop instance running (e.g., HortonWorks VM)
#scp the files to your UNIX environment using a command like this: scp -P 2222 filename hue@127.0.0.1:/usr/lib/hue/
#log in to your UNIX environment: ssh -p 2222 hue@127.0.0.1
#make sure the following files and this script are in the same directory
#a) index_correct_snippet.txt , and b) join_correct_snippet.txt
#make this script executable: chmod +x assignment2_test.sh
#run the script: ./assignment2_test.sh myjarfile.jar /user/hue/

hadoop jar $1 assignment2.WordCount $2 wc_output
hadoop dfs -getmerge $2wc_output wordcount.txt

hadoop jar $1 assignment2.WordFreq $2 wordfreq_output
hadoop dfs -getmerge $2wordfreq_output wordfreq.txt

hadoop jar $1 assignment2.WordFreqStop $2 wordfreqstop_output
hadoop dfs -getmerge $2wordfreqstop_output wordfreqstop.txt

hadoop jar $1 assignment2.Join $2 join_output
hadoop dfs -getmerge $2join_output join.txt

hadoop jar $1 assignment2.Index $2 index_output
hadoop dfs -getmerge $2index_output index.txt

### Task 1

wcresult=`cat wordcount.txt`
if [ $wcresult -eq 13145213 ]
	then
  		echo "Task 1 PASSED"
	else
  		echo "Task 1 FAILED"
fi

### Task 2.1

result=`head -8 wordfreq.txt | tail -1 | awk '{print $1}'`
if [[ $result == "his" ]]
	then 
		echo "Task 2.1 PASSED"
	else 
		echo "Task 2.1 FAILED"
fi

result=`head -1 wordfreq.txt | awk '{print $2}'`
if [ $result -eq 822014 ]
	then 
		echo "Task 2.1 PASSED"
	else 
		echo "Task 2.1 FAILED"
fi

### Task 2.2

result=`head -5 wordfreqstop.txt | tail -1 | awk '{print $2}'`
if [ $result -eq 18492 ]
	then 
		echo "Task 2.2 PASSED"
	else 
		echo "Task 2.2 FAILED"
fi

result=`head -8 wordfreqstop.txt | tail -1 | awk '{print $1}'`
if [[ $result == "love" ]]
	then 
		echo "Task 2.2 PASSED"
	else 
		echo "Task 2.2 FAILED"
fi

### Task 3

cat join_correct_snippet.txt | tr ',' '\n' | sort | sed '/^$/d' | tail -n+4 > join_output_correct.txt
grep '^54173' join.txt > join_output_snippet.txt
grep '^25526651' join.txt >> join_output_snippet.txt
grep '^10015952' join.txt >> join_output_snippet.txt
cat join_output_snippet.txt | tr ',' '\n' | sort | sed '/^$/d' | tail -n+4 > join_output_compare.txt

result=`diff join_output_compare.txt join_output_correct.txt | wc -l`
if [ $result -eq 0 ]
then
  echo "Task 3 PASSED"
else
  echo "Task 3 FAILED"
fi

### Task 4

cat index_correct_snippet.txt | tr '[[:space:]]' '\n' | sort | sed '/^$/d' > index_output_correct.txt
grep "^brando"$'\t' index.txt > index_output_snippet.txt
grep "^pacino"$'\t' index.txt >> index_output_snippet.txt
grep "^coppola"$'\t' index.txt >> index_output_snippet.txt
cat index_output_snippet.txt | tr '[[:space:]]' '\n' | sort | sed '/^$/d' > index_output_compare.txt

result=`diff index_output_compare.txt index_output_correct.txt | wc -l`
if [ $result -eq 0 ]
then
  echo "Task 4 PASSED"
else
  echo "Task 4 FAILED"
fi
