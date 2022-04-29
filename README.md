# Jobsity-Challenge

Steps to get API up and running
1) Create a new folder here to clone the repository.
2) Open a new terminal window and cd into the new folder
3) Clone repository: git clone  https://github.com/erichho/Jobsity-Challenge.git
4) cd into the cloned repository
5) Create a new Python environment: python3 -m venv environment . This will create a new folder named environment
6) Activate environment: source environment/bin/activate .This will activate an isolated Python environment.
7) To install all libraries run: pip install -r requirements.txt
8) Run API: python main.py

With this last step, the API will be running. To test the API, you can open a broweser and try the following endpoints
To upload trips.csv with new data
http://127.0.0.1:5000/uploadCSV/

To Update Database
http://127.0.0.1:5000/updateDatabase/

To check update status
http://127.0.0.1:5000/databaseStatus/

To test finding the weekly average within for a given region
http://127.0.0.1:5000/weeklyAverageRegion/?region=Turin

To test finding the weekly average within a defined box. This tests calculates the weekly average for all Turin trips
http://127.0.0.1:5000/weeklyAverageBox/?lonBox1=7.513035087952872&latBox1=44.976024665620514&lonBox2=7.739760019780325&latBox2=45.13940102848316

Proof the solution is scalable to 100 Million registers. In this link there is the log for processing 100M trips. It took 35 minutes
https://github.com/erichho/Jobsity-Challenge/blob/master/Log%20for%20100M%20trips.png
