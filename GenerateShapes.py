from __future__ import division
import pandas as pd
import geojson as gj
import shapely.geometry as sh
import shapely.ops as ops
from multiprocessing import Process, Manager, Pool
from functools import partial

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
	L.append(gj.Feature(geometry=multi_line,properties={
		"LinePlanningNumber": str(LinePlanningNumber),
		"DataOwnerCode": str(points_joined_segments[points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber]['[DataOwnerCode]'].iloc[0]),
		"LinePublicNumber": str(points_joined_segments[points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber]['[DisplayPublicLine]'].iloc[0]),
		"ProductFormulaType": str(points_joined_segments[points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber]['[ProductFormulaType]'].iloc[0]),
		"TransportType": str(points_joined_segments[points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber]['[TransportType]'].iloc[0]),
		"LineName": str(line_info[line_info['[LinePlanningNumber]'] == LinePlanningNumber]['[LineName]'].iloc[0]),
		"LineColor": str(line_info[line_info['[LinePlanningNumber]'] == LinePlanningNumber]['[LineColor]'].iloc[0])
		}))


if __name__ == "__main__":
	print("Start loading data")
	segments = pd.read_csv('JOPATILIXX.TMI', sep='|',usecols=['[DataOwnerCode]','[LinePlanningNumber]','[JourneyPatternCode]','[TimingLinkOrder]','[UserStopCodeBegin]','[UserStopCodeEnd]','[DisplayPublicLine]','[ProductFormulaType]'])
	pointsOnSegments = pd.read_csv('POOLXXXXXX.TMI', sep='|',usecols=['[UserStopCodeBegin]','[UserStopCodeEnd]','[PointCode]','[DistanceSinceStartOfLink]','[TransportType]'])
	points = pd.read_csv('POINTXXXXX.TMI',sep='|',usecols=['[PointCode]','[LocationX_EW]','[LocationY_NS]'])
	line_info = pd.read_csv('LINEXXXXXX.TMI',sep='|',usecols=['[LinePlanningNumber]','[LineName]','[LineColor]'])
	print("Finished loading data")

	print("Joining the point on links on the segments")
	joined_segments = pd.merge(segments, pointsOnSegments, how="inner", on=['[UserStopCodeBegin]','[UserStopCodeEnd]'])
	print("Joining the points")
	points_joined_segments = pd.merge(joined_segments,points,how="inner",on='[PointCode]')

	with Manager() as manager:
		#Create a list to hold each route's shape to write them to file at the end:
		line_shape_list = manager.list()
		pool = Pool(processes = None)
		func = partial(make_shape,line_shape_list)
		no_lines = len(points_joined_segments['[LinePlanningNumber]'].unique())
		for i, _ in enumerate(pool.imap_unordered(func, points_joined_segments['[LinePlanningNumber]'].unique(), 1)):
			print(str(i+1) + " of " + str(no_lines) + " lines processed",end="\r")
		pool.map(func, points_joined_segments['[LinePlanningNumber]'].unique(),1)
		pool.close()
		pool.join()

		#Finally, write our collection of Features (one for each route) to file in
		#geoJSON format, as a FeatureCollection:
		with open('route_shapes.geojson', 'w') as outfile:
			gj.dump(gj.FeatureCollection(line_shape_list._getvalue()), outfile)