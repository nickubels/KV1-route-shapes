# KV1-route-shapes

This script allows you to generate a GeoJSON with the public transport lines from a Dutch [Koppelvlak 1](http://bison.connekt.nl/standaarden/) schedule. 

### Installing
Install the dependencies using `pip install -r requirements.txt`.

### Usage
Run with `python3 GenrateShapes.py -p PATH` where `PATH` has to be the path to a folder with KV1 files. With `-m` it is possible to check for multiple KV1 folders within the path which will be merged into one GeoJSON. With `-o` it is possible to specify the folder where the GeoJSON has to be saved. See `python3 GenrateShapes.py -h` for more information.

### Example output
![](example.png)
![](qgis.png)

### Important note
In general this script does not take overlapping line segments into account. This means that the output might be less optimised than you would like. The reason for this is that the exact structure of a KV1 differs a bit per agency, it is thus hard to optimise this. As I only used this script for static visualisations I have decided to not further optimise it (at this point)
