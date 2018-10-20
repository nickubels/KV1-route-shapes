# from __future__ import division
import pandas as pd
import geojson as gj
import shapely.geometry as sh
import shapely.ops as ops
import pyproj
from multiprocessing import Process, Manager, Pool
from functools import partial

project = partial(
    pyproj.transform,
    pyproj.Proj(init='epsg:28992'),
    pyproj.Proj(init='epsg:4326'))

def make_shape(L,LinePlanningNumber):
	journey_patterns = set(points_joined_segments[points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber]['[JourneyPatternCode]'])
	subset = points_joined_segments[(points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber)]
	lines = []
	for JourneyPatternCode in journey_patterns:
		timing_link = set(subset[(subset['[JourneyPatternCode]'] == JourneyPatternCode)]['[TimingLinkOrder]'])
		subsubset = subset[(subset['[JourneyPatternCode]'] == JourneyPatternCode)]
		for TimingLinkOrder in timing_link:
			line_string = sh.LineString(zip(subsubset[(subsubset['[TimingLinkOrder]'] == TimingLinkOrder)]['[LocationX_EW]'].tolist(),subsubset[(subsubset['[TimingLinkOrder]'] == TimingLinkOrder)]['[LocationY_NS]'].tolist()))
			if not line_string in lines:
				lines.append(line_string)

	multi_line = sh.MultiLineString(lines)
	merged = ops.linemerge(multi_line)
	merged = ops.transform(project,merged)
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
		#Create a list to hold each route's shape to write them to file at the end:
		line_shape_list = manager.list()
		pool = Pool(processes = None)
		func = partial(make_shape,line_shape_list)
		no_lines = len(points_joined_segments['[LinePlanningNumber]'].unique())
		for i, _ in enumerate(pool.imap_unordered(func, points_joined_segments['[LinePlanningNumber]'].unique(), 1)):
			print("	" + str(i+1) + " of " + str(no_lines) + " lines processed",end="\r")
		pool.close()
		pool.join()

		print("Step 4 out of 4: Writing lines to GeoJSON file")
		#Finally, write our collection of Features (one for each route) to file in
		#geoJSON format, as a FeatureCollection:
		with open('route_shapes.geojson', 'w') as outfile:
			gj.dump(gj.FeatureCollection(line_shape_list._getvalue()), outfile)
	print("Conversion of Koppelvlak 1 to GeoJSON finished")