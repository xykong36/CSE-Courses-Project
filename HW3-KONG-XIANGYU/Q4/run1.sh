hadoop fs -rm -r /user/cse6242/q4output0first_output
hadoop jar ./target/q4-1.0.jar edu.gatech.cse6242.Q4 /user/cse6242/small.tsv /user/cse6242/q4output0
hadoop fs -getmerge /user/cse6242/q4output0/ q4output0.tsv
hadoop fs -rm -r /user/cse6242/q4output0
