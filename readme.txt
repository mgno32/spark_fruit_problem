hadoop jar /usr/lib/hadoop/share/hadoop/tools/lib/hadoop-*streaming*.jar \
-file ~/fruit/code/mapper.py     -mapper ~/fruit/code/mapper.py \
-file ~/fruit/code/reducer.py    -reducer ~/fruit/code/reducer.py \
-input ~/fruit/hdfs_in/*    -output ~/fruit/hdfs_out
python count.py >> rec.txt
