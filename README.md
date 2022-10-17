# CS441_Fall2022
Welcome to my submission for Homework 1. Below I will layout the files in my program and their uses.

# Configuration Files (src/main/resources) 
Added configuration files much like the one sincluded for LogFileGenerator as HW1Configs.conf. This file allows users to write in specific input and output directories for each of the tasks. And using a utility is fetched inside the program for use. It also allows for the passing of parameters that change the task's behavior likeinputting a time interval for limiting the logs mapped.

Also added a logger to logback.xml which is already used by the LogFileGenerator, this logger is created inside the main scala file and its output is written to log files inside the MRLogs directory.

# Utility Objects HW1Utils (src/main/scala/HW1Utils)
Adding configruation files and my own logger meant that I had to create objects for fetching them just like is already done for LogFileGenerator. I needed to create logger, obtain the reference for the config files and have an object fetch the paramters for the program as requested. 

The logger creation and obtaining the config reference is very similar the biggest difference in these files is using HW1Params. 
An example of using the input directory for task 1 is: HW1Params.inputPathTask1.




