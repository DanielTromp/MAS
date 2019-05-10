# MAS
## Command:
    python script-mas-dii.py -f http://localhost:8080/ -t /tmp/ \
        -i localhost -u root -p masPassword -d imagedb --test -v
    python script-mas-dii.py
## Creator:
    'Daniel Tromp' <drpgmtromp@gmail.com>
## Project, part:
    Media Analyses Service (MAS) Pipeline, Data Information Ingestion (DII)
## Project information:
    Get more custom tailort information from existing and live data.
    "Modular (containerized) scaleble data processing pipeline infrastructure"
## Used for:
    Initialize and append file information to DB library for future processing
## Description:
    Check every image on file server and add it to database table,
    gather as much information on the images as possible.
    Every image has a sha256 hash to prevent remove duplicates,
    duplicate locations will be stored with the first entry.
    The sha256 column is unique (foreign key).
## Datapoints:
    File name/size/location, EXIF data, SHA256.
