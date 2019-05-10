#!/usr/bin/env python

''' 
Command:
    python script-mas-dii.py -f http://localhost:8080/ -t /tmp/ \
        -i localhost -u root -p masPassword -d imagedb --test -v
    python script-mas-dii.py
Creator:
    'Daniel Tromp' <drpgmtromp@gmail.com>
Project, part:
    Media Analyses Service (MAS) Pipeline, Data Information Ingestion (DII)
Project information:
    Get more custom tailort information from existing and live data.
    "Modular (containerized) scaleble data processing pipeline infrastructure"
Used for:
    Initialize and append file information to DB library for future processing
Description:
    Check every image on file server and add it to database table,
    gather as much information on the images as possible.
    Every image has a sha256 hash to prevent remove duplicates,
    duplicate locations will be stored with the first entry.
    The sha256 column is unique (foreign key).
Datapoints:
    File name/size/location, EXIF data, SHA256.
'''

from urllib.request import Request, urlopen, urlretrieve
from mysql.connector import connect
from bs4 import BeautifulSoup
from hashlib import sha256
from time import strftime
from PIL import ExifTags
from PIL import Image
from re import findall
import argparse
import os

# construct the argument parser and parse the arguments
ap = argparse.ArgumentParser()
ap.add_argument("-f", "--fileserver", 
    default="http://localhost:8080/", 
    help="path to the file server")
ap.add_argument("-t", "--temp", 
    default="/tmp/", 
    help="path to temp directory")
ap.add_argument("-i", "--host", 
    default="localhost", 
    help="path to database")
ap.add_argument("-u", "--user", 
    default="root", 
    help="database username")
ap.add_argument("-p", "--password", 
    default="masPassword", 
    help="database user password")
ap.add_argument("-d", "--database", 
    default="imagedb", 
    help="database name")
ap.add_argument("--test",
    action="store_true",
    default=False,
    help="For testing purposes only, create/drop the db table")
ap.add_argument("-v", "--verbose",
    action="store_true",
    default=False,
    help="verbose")
args = vars(ap.parse_args())

# Declair general use variables
file_http = args["fileserver"]
tmp_dir = args["temp"]
db = connect(   host=args["host"], 
                user=args["user"], 
                passwd=args["password"], 
                database=args["database"])
cursor = db.cursor()
image_file_types = ('.png', '.jpg', '.jpeg', '.tiff', 
                    '.tif', '.bmp', '.gif', '.jfif')


# Rebuild the DB for testing
def rebuild_db():
    columns = """(id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                  date_inserted DATETIME,
                  file_image BOOLEAN,
                  file_name VARCHAR(255),
                  file_name_org VARCHAR(255),
                  location VARCHAR(255),
                  location_duplicates VARCHAR(1024),
                  file_size INT(11),
                  sha256 VARBINARY(64) UNIQUE,
                  exif_data TEXT)""" 
    try:
        cursor.execute("DROP TABLE photos")
        if args["verbose"]:
            print("table dropped")
    except:
        if args["verbose"]:
            print("table doesn't exist")
    try:
        cursor.execute("CREATE TABLE photos " + columns)
        if args["verbose"]:
            print("table created")
    except:
        if args["verbose"]:
            print("table not created")

# Write dictionary into db table row
def write_db(myDict):
    try:
        placeholders = ', '.join(['%s'] * len(myDict))
        columns = ', '.join(myDict.keys())
        query = """INSERT INTO photos ( %s ) VALUES ( %s )
                    """ % (columns, placeholders)
        cursor.execute(query, list(myDict.values()))
        if args["verbose"]:
            print(cursor.rowcount, "entry created")
    except:
        query = "SELECT location_duplicates FROM photos WHERE sha256 = %s"
        sha = (myDict['sha256'],)
        cursor.execute(query, sha)
        records = cursor.fetchone()
        records = ', '.join(records).replace(",", " ")

        if myDict['location_duplicates'] in records:
            if args["verbose"]:
                print("entry exists")
        else:
            query = """UPDATE photos SET location_duplicates = 
                    CONCAT(location_duplicates, ', ' %s) WHERE sha256 = %s"""
            values = (myDict['location_duplicates'], myDict['sha256'])
            cursor.execute(query, values)
            if args["verbose"]:
                print("entry location_duplicates appended")
    finally:
        db.commit()
        return None

# Check sha256 for duplicates and changes
def sha256sum(_file_name):
    h  = sha256()
    b  = bytearray(128*1024)
    mv = memoryview(b)
    with open(_file_name, 'rb', buffering=0) as f:
        for n in iter(lambda: f.readinto(mv), 0):
            h.update(mv[:n])
    return h.hexdigest()

# Get EXIF data from file if it exists
def exif(local_file):
    exifData = {}
    try:
        img = Image.open(local_file)
        exifDataRaw = img._getexif()
        for tag, value in exifDataRaw.items():
            decodedTag = ExifTags.TAGS.get(tag, tag)
            if decodedTag == 'MakerNote':
                continue
            else:
                exifData[decodedTag] = value
        return(exifData)
    except:
        return None

def main(file_http):
    # Get all column names from the photos table and remove id
    cursor.execute("desc photos")
    col = ([column[0] for column in cursor.fetchall()])
    columns = []
    for c in col:
        if c != 'id':
            columns.append(c)
    # Fill dictionary with keys
    myDict = {}
    myDict = dict.fromkeys(columns,)

    # Get file from file (web) server and inspect
    url = file_http.replace(" ","%20")
    req = Request(url)
    a = urlopen(req).read().decode("utf-8")
    soup = BeautifulSoup(a, 'html.parser')
    x = (soup.find_all('a'))
    for i in x:
        _file_name = findall('"([^"]*)"', str(i))[0]
        _file_name_org = i.extract().get_text()
        url_new = url + _file_name
        url_new = url_new.replace(" ","%20")
        if(_file_name[-1]=='/'): # and _file_name[0]!='.'):
            main(url_new)
        if (_file_name[-1]!='/'):
            local_file = tmp_dir + _file_name
            urlretrieve(url_new, local_file)

            ### Fill dictionay with values ###
            if _file_name.lower().endswith((image_file_types)):
                myDict.update({'file_image': True})
            else:
                myDict.update({'file_image': False})
            myDict.update({'file_name': _file_name})
            myDict.update({'file_name_org': _file_name_org})
            myDict.update({'file_size': os.stat(local_file).st_size})
            myDict.update({'location': url_new})
            myDict.update({'location_duplicates': url_new})
            myDict.update({'sha256': sha256sum(local_file)})
            myDict.update({'exif_data': str(exif(local_file))})
            myDict.update({'date_inserted': strftime('%Y-%m-%d %H:%M:%S')})

            # Clean up
            os.remove(local_file)
            # Write dictionary into db table row
            write_db(myDict)

# Drop and rebuild table clean,
# only use this for testing.
if args["test"]:
    rebuild_db()

### Starting the main script ###
main(file_http)

# Close the db and cursor connection, because...
cursor.close()
db.close()
