#Project Description
The purpose of this research is to identify whether age, race and gender
influence Nutrition intake in the American Population based on NHANES
data. Nowadays fat becomes gradually popular. Therefore I analyze ten type
of main nutrient impact by using 2014 the newest version NHANES data. I do
Preprocessing data and Clustering with K-means analysis by using
MapReduce programming model, Finally prompt a group of people to pay
attention to there Nutrition Intake by analyzing k cluster center.

# NHANES-Downloader
Python script to download the entire NHANES dataset from the CDC website.
Additionally, a script is provided to convert the downloaded file format (XPT)
to CSV using the Pandas library for Python 3.

# Requirements
* Python 3
* Pandas
* BeautifulSoup

The easiest way to ensure that these requirements are met is to install
Anaconda 3.  You can download that from here:
https://www.continuum.io/downloads

If you already have python 3 installed, then Pandas and BeautifulSoupr can be
installed using pip3 with `$ pip3 install --user pandas` and `$ pip3 install --user
beautifulsoup4`


# Description
This is a simple python script which you can use to download the entire NHANES
dataset from the CDC website.  The script will load the websites at the URLs
provided in `NHANES_URLS.txt` and parse each page for links to .XPT files.  It
will then download these files and store then in a local `./data/raw_data/`
directory.  An additional script is provided which converts these .XPT files
to .CSV files for easier use of the NHANES data.  This script will output to
the files to `./data/csv_data/` by default.

# Usage
Running the script is easy.  Just navigate to the directory of the script and
then:
```
$ ./get_data.py
```
To convert the data in `./data/raw_data/` to .CSV format, run the following
from the same directory as the first sript:
```
$ ./raw_to_csv.py
```

# Arguments
There are a few command line arguments for this script (but default values have
been set).  `-o` will specify the directory to save the NHANES data. `-m` can
be used to invoke a multiprocess version of the script which utilizes the
python `multiprocessing.pool` method.  Last, you can specify a different file
containing URLs.  For additional documentation, try:
```
$ ./get_data.py -h
```
The additional `raw_to_csv.py` script has several command line options as well
(but default values have been set).  '-i' should give the location of the
directory containing .XPT files.  '-o' will specify the directory to save the
NHANES data in CSV format.  `-m` can be used to invoke a multiprocess version
of the script which utilizes the python `multiprocessing.pool` method.  For
additional documentation, try:
```
$ ./raw_to_csv.py -h
```
