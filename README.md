# KV1-route-shapes

This script allows you to generate a GeoJSON with the public transport lines from a Dutch [Koppelvlak 1](http://bison.connekt.nl/standaarden/) schedule. 

### Installing
Install the dependencies using `pip install -r requirements.txt`.

### Usage
Run with `python3 GenrateShapes.py -p PATH` where `PATH` has to be the path to a folder with KV1 files. With `-m` it is possible to check for multiple KV1 folders within the path which will be merged into one GeoJSON. With `-o` it is possible to specify the folder where the GeoJSON has to be saved. See `python3 GenrateShapes.py -h` for more information.

### Example output
![](example.png)
![](qgis.png)
