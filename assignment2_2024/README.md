# TDT4225 Group 35, Assignment 2 
In this assignment we are working with the Geolife GPS Trajectory dataset from Microsoft. Our goal is to create tables in a database that we insert data we have processed from the dataset. In this project we will be using MySQL and python. As setup we have recieved access to a virtual machine at IDI’s cluster, running Ubuntu. 



**How to run the code**
- Download the Geolife GPS Trajectory from Microsoft. (We are using a special version of this provided by NTNU, the original dataset may give errors due to differences in datastructure etc)
- Update the values in the DBConnector.py file to connect to your own database.
- Run in terminal: pip install python 
- Run in terminal: pip install -r requirements.txt
- Run in terminal: python3 GeoLifeTask.py to run the script. 



**Overview of the project**
- Our main script is the GeoLifeTask.py. Running this will create tables in our database, process the data and then insert the data into the tables in our database.
- Task 2 is implemented in our query.py file. Here, all the answers listed in task 2 is being answered. 
