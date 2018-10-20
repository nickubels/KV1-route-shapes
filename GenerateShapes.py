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
from multiprocessing import Process, Manager, Pool
from functools import partial

# Define a partial function for transforming from RD_New to WGS84 to comply with GeoJSON standards
project = partial(
    pyproj.transform,
    pyproj.Proj(init='epsg:28992'), #RD_new
    pyproj.Proj(init='epsg:4326'))	#WGS84


# Define the function that generates the shapes
def make_shape(L,LinePlanningNumber):
	# Extract a list with all Journey Patterns
	journey_patterns = set(points_joined_segments[points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber]['[JourneyPatternCode]'])
	# Create a subset of segments with this specific LinePlanningNumber
	subset = points_joined_segments[(points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber)]
	# Create an empty list to store the 
	lines = []
	for JourneyPatternCode in journey_patterns:
		# Create a list with timing link numbers
		timing_link = set(subset[(subset['[JourneyPatternCode]'] == JourneyPatternCode)]['[TimingLinkOrder]'])
		# Create a subset with this specific JourneyPatternCode
		subsubset = subset[(subset['[JourneyPatternCode]'] == JourneyPatternCode)]
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
		"DataOwnerCode": str(points_joined_segments[points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber]['[DataOwnerCode]'].iloc[0]),
		"LinePublicNumber": str(points_joined_segments[points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber]['[DisplayPublicLine]'].iloc[0]),
		"ProductFormulaType": str(points_joined_segments[points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber]['[ProductFormulaType]'].iloc[0]),
		"TransportType": str(points_joined_segments[points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber]['[TransportType]'].iloc[0]),
		"LineName": str(line_info[line_info['[LinePlanningNumber]'] == LinePlanningNumber]['[LineName]'].iloc[0]),
		"LineColor": str(line_info[line_info['[LinePlanningNumber]'] == LinePlanningNumber]['[LineColor]'].iloc[0])
		}))


if __name__ == "__main__":
	print("Koppelvlak 1 to GeoJSON\nhttps://github.com/nickubels/KV1-route-shapes")
	print("Step 1 out of 4: Start loading data")
	# Try to load the data containing the route segment data
	try:
		print("	Attempting to load JOPATILIXX.TMI")
		segments = pd.read_csv('JOPATILIXX.TMI', sep='|',usecols=['[DataOwnerCode]','[LinePlanningNumber]','[JourneyPatternCode]','[TimingLinkOrder]','[UserStopCodeBegin]','[UserStopCodeEnd]','[DisplayPublicLine]','[ProductFormulaType]'])
		print("	Finished loading JOPATILIXX.TMI")
	except FileNotFoundError:
		print("Error: Could not find JOPATILIXX.TMI")
		exit()
	# Try to load the data containing the points on these route segments
	try:
		print("	Attempting to load POOLXXXXXX.TMI")
		pointsOnSegments = pd.read_csv('POOLXXXXXX.TMI', sep='|',usecols=['[UserStopCodeBegin]','[UserStopCodeEnd]','[PointCode]','[DistanceSinceStartOfLink]','[TransportType]'])
		print("	Finished loading POOLXXXXXX.TMI")
	except FileNotFoundError:
		print("Error: Could not find POOLXXXXXX.TMI")
		exit()
	# Try to load the data containing the actual coordinates of the points
	try:
		print("	Attempting to load POINTXXXXX.TMI")
		points = pd.read_csv('POINTXXXXX.TMI',sep='|',usecols=['[PointCode]','[LocationX_EW]','[LocationY_NS]'])
		print("	Finished loading POINTXXXXX.TMI")
	except FileNotFoundError:
		print("Error: Could not find POINTXXXXX.TMI")
		exit()
	# Try to load extra data on the line
	try:
		print("	Attempting to load LINEXXXXXX.TMI")
		line_info = pd.read_csv('LINEXXXXXX.TMI',sep='|',usecols=['[LinePlanningNumber]','[LineName]','[LineColor]'])
		print("	Finished loading LINEXXXXXX.TMI")
	except FileNotFoundError:
		print("Error: Could not find LINEXXXXXX.TMI")
		exit()

	print("Step 2 out of 4: Joining data together")
	print("	Joining the points on the segments to the segments")
	joined_segments = pd.merge(segments, pointsOnSegments, how="inner", on=['[UserStopCodeBegin]','[UserStopCodeEnd]'])
	print("	Joining the coordinates to those points")
	points_joined_segments = pd.merge(joined_segments,points,how="inner",on='[PointCode]')

	with Manager() as manager:
		print("Step 3 out of 4: Generating the lines")
		# Create a list to hold each route's shape to write them to file at the end
		line_shape_list = manager.list()
		# Create the pool
		pool = Pool(processes = None)
		# Create the partial function for handling each line
		func = partial(make_shape,line_shape_list)
		# Calculate the amount of lines for displaying progress
		no_lines = len(points_joined_segments['[LinePlanningNumber]'].unique())
		# Execute calculations for each line and print status
		for i, _ in enumerate(pool.imap_unordered(func, points_joined_segments['[LinePlanningNumber]'].unique(), 1)):
			print("	" + str(i+1) + " of " + str(no_lines) + " lines processed",end="\r")
		# End processing
		pool.close()
		pool.join()

		print("Step 4 out of 4: Writing lines to GeoJSON file")
		# Write our collection of Features (one for each route) to file in
		# GeoJSON format, as a FeatureCollection:
		with open('route_shapes.geojson', 'w') as outfile:
			gj.dump(gj.FeatureCollection(line_shape_list._getvalue()), outfile)
	print("Conversion of Koppelvlak 1 to GeoJSON finished")