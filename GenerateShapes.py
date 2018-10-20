import pandas as pd
import geojson as gj
import shapely.geometry as sh
import shapely.ops as ops
from multiprocessing import Process, Manager, Pool
from functools import partial

def make_shape(L,LinePlanningNumber):
	journey_patterns = set(points_joined_segments[points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber]['[JourneyPatternCode]'])
	lines = []
	for JourneyPatternCode in journey_patterns:
		timing_link = set(points_joined_segments[(points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber) & (points_joined_segments['[JourneyPatternCode]'] == JourneyPatternCode)]['[TimingLinkOrder]'])
		print(timing_link)
		for TimingLinkOrder in timing_link:
			lines.append(sh.LineString(zip(points_joined_segments[(points_joined_segments['[TimingLinkOrder]'] == TimingLinkOrder) & (points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber) & (points_joined_segments['[JourneyPatternCode]'] == JourneyPatternCode)]['[LocationX_EW]'].tolist(),points_joined_segments[(points_joined_segments['[TimingLinkOrder]'] == TimingLinkOrder) & (points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber) & (points_joined_segments['[JourneyPatternCode]'] == JourneyPatternCode)]['[LocationY_NS]'].tolist())))
		# line_segments = points_joined_segments[(points_joined_segments['[LinePlanningNumber]'] == LinePlanningNumber) & (points_joined_segments['[JourneyPatternCode]'] == JourneyPatternCode)]['[TimingLinkOrder]']
		# print(line_segments)

	multi_line = sh.MultiLineString(lines)
	merged = ops.linemerge(multi_line)
	L.append(gj.Feature(geometry=multi_line,properties={
		"LinePlanningNumber": str(LinePlanningNumber)
		}))


if __name__ == "__main__":
	print("Start loading data")
	segments = pd.read_csv('JOPATILIXX.TMI', sep='|',usecols=['[DataOwnerCode]','[LinePlanningNumber]','[JourneyPatternCode]','[TimingLinkOrder]','[UserStopCodeBegin]','[UserStopCodeEnd]','[DisplayPublicLine]','[ProductFormulaType]'])
	pointsOnSegments = pd.read_csv('POOLXXXXXX.TMI', sep='|',usecols=['[UserStopCodeBegin]','[UserStopCodeEnd]','[PointCode]','[DistanceSinceStartOfLink]'])
	points = pd.read_csv('POINTXXXXX.TMI',sep='|',usecols=['[PointCode]','[LocationX_EW]','[LocationY_NS]'])
	print("Finished loading data")

	print("Joining the point on links on the segments")
	joined_segments = pd.merge(segments, pointsOnSegments, how="inner", on=['[UserStopCodeBegin]','[UserStopCodeEnd]'])
	print("Joining the points")
	points_joined_segments = pd.merge(joined_segments,points,how="inner",on='[PointCode]')
	print(points_joined_segments)
	# points_joined_segments.drop_duplicates(subset=['[LinePlanningNumber]','[UserStopCodeBegin]','[UserStopCodeEnd]'],inplace=True)

	with Manager() as manager:
		#Create a list to hold each route's shape to write them to file at the end:
		line_shape_list = manager.list()
		pool = Pool(processes = None)
		func = partial(make_shape,line_shape_list)
		pool.map(func, points_joined_segments['[LinePlanningNumber]'].unique(),1)
		pool.close()
		pool.join()

		#Finally, write our collection of Features (one for each route) to file in
		#geoJSON format, as a FeatureCollection:
		with open('route_shapes.geojson', 'w') as outfile:
			gj.dump(gj.FeatureCollection(line_shape_list._getvalue()), outfile)