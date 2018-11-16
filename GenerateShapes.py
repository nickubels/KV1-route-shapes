# Nick Ubels
# https://github.com/nickubels/KV1-route-shapes
# Generates a GeoJSON based on lines defined in Koppelvlak 1 files
# Run this script from within a directory containing the Koppelvlak 1 TMI files

# Import all the modules we need
import pandas as pd
import geojson as gj
import shapely.geometry as sh
import shapely.ops as ops
import pyproj
import argparse
import os
from multiprocessing import Process, Manager, Pool
from functools import partial


def get_args():
    parser = argparse.ArgumentParser(description='Koppelvlak 1 to GeoJSON\nhttps://github.com/nickubels/KV1-route-shapes')
    parser.add_argument('--path', '-p',metavar='FOLDER',help="Specify the path in which the files or subfolders are stored",required=True)
    parser.add_argument('--output', '-o',metavar='FOLDER',help="Specify the path where the GeoJSON should be stored (Default: working directory)",default='')
    parser.add_argument('--multiple','-m',action='store_true', default=False, help="Wether multiple KV1 folders should be read (Default: False)")
    return parser.parse_args()

# Define a partial function for transforming from RD_New to WGS84 to comply with GeoJSON standards
project = partial(
    pyproj.transform,
    pyproj.Proj(init='epsg:28992'), #RD_new
    pyproj.Proj(init='epsg:4326'))  #WGS84

def load_data(path):
    print("Step 1 out of 3: Start loading data")
    # Try to load the data containing the route segment data
    try:
        print(" Attempting to load JOPATILIXX.TMI")
        segments = pd.read_csv(os.path.join(path,'JOPATILIXX.TMI'), sep='|',usecols=['[DataOwnerCode]','[LinePlanningNumber]','[JourneyPatternCode]','[TimingLinkOrder]','[UserStopCodeBegin]','[UserStopCodeEnd]','[DisplayPublicLine]','[ProductFormulaType]'])
        print(" Finished loading JOPATILIXX.TMI")
    except FileNotFoundError:
        raise Exception("Could not find JOPATILIXX.TMI")
        # exit()
    # Try to load the data containing the points on these route segments
    try:
        print(" Attempting to load POOLXXXXXX.TMI")
        pointsOnSegments = pd.read_csv(os.path.join(path,'POOLXXXXXX.TMI'), sep='|',usecols=['[UserStopCodeBegin]','[UserStopCodeEnd]','[PointCode]','[DistanceSinceStartOfLink]','[TransportType]'])
        print(" Finished loading POOLXXXXXX.TMI")
    except FileNotFoundError:
        raise Exception("Could not find POOLXXXXXX.TMI")
        # exit()
    # Try to load the data containing the actual coordinates of the points
    try:
        print(" Attempting to load POINTXXXXX.TMI")
        points = pd.read_csv(os.path.join(path,'POINTXXXXX.TMI'),sep='|',usecols=['[PointCode]','[LocationX_EW]','[LocationY_NS]'])
        print(" Finished loading POINTXXXXX.TMI")
    except FileNotFoundError:
        raise Exception("Could not find POINTXXXXX.TMI")
        # exit()
    # Try to load extra data on the line
    try:
        print(" Attempting to load LINEXXXXXX.TMI")
        line_info = pd.read_csv(os.path.join(path,'LINEXXXXXX.TMI'),sep='|',usecols=['[LinePlanningNumber]','[LineName]','[LineColor]'])
        print(" Finished loading LINEXXXXXX.TMI")
    except FileNotFoundError:
        raise Exception("Could not find LINEXXXXXX.TMI")
        # exit()

    print("Step 2 out of 3: Joining data together")
    print(" Joining the points on the segments to the segments")
    joined_segments = pd.merge(segments, pointsOnSegments, how="inner", on=['[UserStopCodeBegin]','[UserStopCodeEnd]'])
    print(" Joining the coordinates to those points")
    points_joined_segments = pd.merge(joined_segments,points,how="inner",on='[PointCode]')

    return (line_info,points_joined_segments)

# Define the function that generates the shapes
def make_shape(LinePlanningNumber,L,info,segments):
    # Extract a list with all Journey Patterns
    journey_patterns = set(segments[segments['[LinePlanningNumber]'] == LinePlanningNumber]['[JourneyPatternCode]'])
    # Create a subset of segments with this specific LinePlanningNumber
    subset = segments[(segments['[LinePlanningNumber]'] == LinePlanningNumber)]
    # Create an empty list to store the 
    lines = []
    for JourneyPatternCode in journey_patterns:
        # Create a list with timing link numbers
        timing_link = set(subset[(subset['[JourneyPatternCode]'] == JourneyPatternCode)]['[TimingLinkOrder]'])
        # Create a subset with this specific JourneyPatternCode and make sure they are in the correct order
        subsubset = subset[(subset['[JourneyPatternCode]'] == JourneyPatternCode)].sort_values('[DistanceSinceStartOfLink]')
        for TimingLinkOrder in timing_link:
            # Zip the points together to form a linestring
            line_string = sh.LineString(zip(subsubset[(subsubset['[TimingLinkOrder]'] == TimingLinkOrder)]['[LocationX_EW]'].tolist(),subsubset[(subsubset['[TimingLinkOrder]'] == TimingLinkOrder)]['[LocationY_NS]'].tolist()))
            # Check if the linestring does not already exist in the line
            if not line_string in lines:
                # If that is the case then append it
                lines.append(line_string)

    # Make a MultiLineString of the different parts
    multi_line = sh.MultiLineString(lines)
    # Preform a linemerge
    merged = ops.linemerge(multi_line)
    # Transform from RD_new to WGS84
    merged = ops.transform(project,merged)
    # Append feature to List
    L.append(gj.Feature(geometry=merged,properties={
        "LinePlanningNumber": str(LinePlanningNumber),
        "DataOwnerCode": str(segments[segments['[LinePlanningNumber]'] == LinePlanningNumber]['[DataOwnerCode]'].iloc[0]),
        "LinePublicNumber": str(segments[segments['[LinePlanningNumber]'] == LinePlanningNumber]['[DisplayPublicLine]'].iloc[0]),
        "ProductFormulaType": str(segments[segments['[LinePlanningNumber]'] == LinePlanningNumber]['[ProductFormulaType]'].iloc[0]),
        "TransportType": str(segments[segments['[LinePlanningNumber]'] == LinePlanningNumber]['[TransportType]'].iloc[0]),
        "LineName": str(info[info['[LinePlanningNumber]'] == LinePlanningNumber]['[LineName]'].iloc[0]),
        "LineColor": str(info[info['[LinePlanningNumber]'] == LinePlanningNumber]['[LineColor]'].iloc[0])
        }))

def handle_agency(data_list,path):
    try:
        info,segments = load_data(path)
        # Create the pool
        pool = Pool(processes = None)
        # Create the partial function for handling each line
        func = partial(make_shape,L=data_list,info=info,segments=segments)
        # Calculate the amount of lines for displaying progress
        no_lines = len(segments['[LinePlanningNumber]'].unique())
        # Execute calculations for each line and print status
        for i, _ in enumerate(pool.imap_unordered(func, segments['[LinePlanningNumber]'].unique(), 1)):
            print(" " + str(i+1) + " of " + str(no_lines) + " lines processed",end="\r")
        print("")
        # End processing
        pool.close()
        pool.join()
    except Exception as e:
        print("Something went wrong: " + str(e))

if __name__ == "__main__": 
    args = get_args()

    with Manager() as manager:
        line_shape_list = manager.list()

        if args.multiple:
            subfolders = [x[0] for x in os.walk(args.path)]
            for folder in subfolders:
                if folder is not args.path:
                    print("KV1 folder in " + folder)
                    handle_agency(line_shape_list, folder)
        else:
            print("KV1 folder in " + args.path)
            handle_agency(line_shape_list, args.path)    

        print("Writing lines to GeoJSON file")
        # Write our collection of Features (one for each route) to file in
        # GeoJSON format, as a FeatureCollection:
        with open(os.path.join(args.output,'route_shapes.geojson'), 'w') as outfile:
            gj.dump(gj.FeatureCollection(line_shape_list._getvalue()), outfile)
    print("Conversion of Koppelvlak 1 to GeoJSON finished")