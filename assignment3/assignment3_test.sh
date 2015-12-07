#!/bin/bash

#arguments
#1. path to spark distribution
#	e.g., /home/username/spark-1.5.2-bin-hadoop2.6
#2. path to jar file
#	e.g., /home/username/olatusername.jar
#3. path to log file
#	e.g., /home/username/log_file.xml
#4. path to plot_summaries file
#	e.g., /home/username/plot_summaries.txt

# run it like this:
# chmod +x test_script.sh
# ./test_script.sh spark-1.5.2-bin-hadoop2.6 bpaudel.jar longer_log.xml plot_summaries.txt

SPARK_DIR=$1
JARFILE=$2
LOGFILE=$3
PLOTFILE=$4
OUTPUTFILE=assignment3_testscript_output.txt

### Task 1
$SPARK_DIR/bin/spark-submit --class assignment3.WordFreq --master local $JARFILE $4 wordfreq_output.txt

sampleWordFreqResult=`head -8 wordfreq_output.txt | tail -1 | awk '{print $1}'`
if [[ $sampleWordFreqResult == "his" ]]
	then 
		echo "Task 1.1 PASSED" > $OUTPUTFILE
	else 
		echo "Task 1.1 FAILED" > $OUTPUTFILE
fi

sampleWordFreqResult=`head -1 wordfreq_output.txt | awk '{print $2}'`
if [ $sampleWordFreqResult -eq 736050 ]
	then 
		echo "Task 1.2 PASSED" >> $OUTPUTFILE
	else 
		echo "Task 1.2 FAILED" >> $OUTPUTFILE
fi

### Task 2
$SPARK_DIR/bin/spark-submit --class assignment3.AverageTime --master local $JARFILE $LOGFILE avgtime_output.txt

avgtime=`cat avgtime_output.txt`
diff_avg=`echo "$avgtime-40693" | bc`
if (( $(bc <<< "$diff_avg > -2000") && $(bc <<< "$diff_avg < 2000") ))
	then 
		echo "Task 2 PASSED" >> $OUTPUTFILE
	else 
		echo "Task 2 FAILED" >> $OUTPUTFILE
fi

### Task 3
$SPARK_DIR/bin/spark-submit --class assignment3.CommandCount --master local $JARFILE $LOGFILE cmdcount_output.txt
sampleCmdCount=`head -5 cmdcount_output.txt | tail -1 | awk '{print $2}'`
if [ $sampleCmdCount -eq 110 ]
	then 
		echo "Task 3.1 PASSED" >> $OUTPUTFILE
	else 
		echo "Task 3.1 FAILED" >> $OUTPUTFILE
fi

sampleCmdCount=`head -1 cmdcount_output.txt | awk '{print $2}'`
if [ $sampleCmdCount -eq 4 ]
	then 
		echo "Task 3.2 PASSED" >> $OUTPUTFILE
	else 
		echo "Task 3.2 FAILED" >> $OUTPUTFILE
fi

### Task 4
$SPARK_DIR/bin/spark-submit --class assignment3.CommandFrequency --master local $JARFILE $LOGFILE cmdfreq_output.txt
sampleCmdFreq=`head -9 cmdfreq_output.txt | tail -1 | awk '{print $1}'`
if [[ $sampleCmdFreq == "AssistCommand" ]]
	then 
		echo "Task 4.1 PASSED" >> $OUTPUTFILE
	else 
		echo "Task 4.1 FAILED" >> $OUTPUTFILE
fi

sampleCmdFreq=`head -9 cmdfreq_output.txt | tail -1 | awk '{print $2}'`
if [ $sampleCmdFreq -eq 7 ]
	then 
		echo "Task 4.2 PASSED" >> $OUTPUTFILE
	else 
		echo "Task 4.2 FAILED" >> $OUTPUTFILE
fi

sampleCmdFreq=`head -15 cmdfreq_output.txt | tail -1 | awk '{print $1}'`
if [[ $sampleCmdFreq == "UndoCommand" ]]
	then 
		echo "Task 4.3 PASSED" >> $OUTPUTFILE
	else 
		echo "Task 4.3 FAILED" >> $OUTPUTFILE
fi

sampleCmdFreq=`head -15 cmdfreq_output.txt | tail -1 | awk '{print $2}'`
if [ $sampleCmdFreq -eq 2 ]
	then 
		echo "Task 4.4 PASSED" >> $OUTPUTFILE
	else 
		echo "Task 4.4 FAILED" >> $OUTPUTFILE
fi

sampleCmdFreq=`head -1 cmdfreq_output.txt | awk '{print $1}'`
if [[ $sampleCmdFreq == "MoveCaretCommand" ]]
	then 
		echo "Task 4.5 PASSED" >> $OUTPUTFILE
	else 
		echo "Task 4.5 FAILED" >> $OUTPUTFILE
fi

sampleCmdFreq=`head -1 cmdfreq_output.txt | awk '{print $2}'`
if [ $sampleCmdFreq -eq 78 ]
	then 
		echo "Task 4.6 PASSED" >> $OUTPUTFILE
	else 
		echo "Task 4.6 FAILED" >> $OUTPUTFILE
fi
