# Data Processing & Import

### Usage

- Edit script file in /scripts, for example /scripts/2009.py to perform data processing and upload data to openTSDB.

- Executing Main.py {year_1, year_2, ...} to upload script to server then run it.

### Refs

- Path of uploading is /var/data/ while the path of data file is /var/data/TaxiData/.

- All time must use timestamp.

- 2013.py and 2014.py have been finished. 2013 data is an example for raw gps data while 2014 is for od data.

### Methods

#### Spatial Unit

- init: Initialize instance by *shp_path*.

- find_unit_by_point: Find unit by *lon* and *lat*, return unit id or -1(not exist).

#### DB Operation

- put_one: Upload one data point to db by *metric*, *value*, *tags* and *timestamp*.

- put: Upload data in bulks using long session. *data* and *branch_szie* needed.

- query: Query for data by *start*, *end* and *queries*.

- Query: A class for query request, instanced by *metric*(necessary), *tags*, *agg*(aggregator), *filters*(filter for tags) and *downsample*(time interval and function for down sample).



#### Unzip

- un_zip: Unzip .zip file by *filename* and return name of folder.

- un_gz: Unzip .gz file by *filename* and return name of folder.

- un_tar: Unzip .tar file by *filename* and return name of folder.

- un_tar_gz: Unzip .tar.gz file by *filename* and return name of folder.