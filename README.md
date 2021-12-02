# CS1660-HW5
For this project, I implemented a mapreduce job that generates the top N words based on wordcount. It uses a single mapreduce job to accomplish this, with each mapper generating the top N words for a given input split and the reducer generating the global top N of all reducer outputs. The program assumes all files are in a folder and there are no subfolders. To do this, I took the input files from course project part 2 and extracted them all into a single folder

## Screenshot of GCP account
![GCP Account](./img/gcpaccount.PNG)


## Screenshot of mapreduce job on GCP
![Wordcount job](./img/wcoutput.PNG)


## Output file
The results of my mapreduce algorithm can be found in the `results.txt` file
