# CS1660-HW5
For this project, I implemented a mapreduce job that generates the top N words based on wordcount. It uses a single mapreduce job to accomplish this, with each mapper generating the local top N words for a given input split and the reducer generating the global top N of all mapper outputs. The output of my mapreduce job is in reversed order, since I used a treemap to store the top N values. The output itself is subject to variance given how the local top N lists are generated in the mappers. Also, the program assumes all files are in a folder and there are no subfolders. To do this, I took the input files from course project part 2 and extracted them all into a single folder. 

## Screenshot of GCP account
![GCP Account](./img/gcpaccount.PNG)


## Screenshot of mapreduce job on GCP
![Wordcount job](./img/wcoutput.PNG)


## Output file
The results of my mapreduce algorithm can be found in the `results.txt` file
