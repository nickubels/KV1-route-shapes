import pandas as pd
import geojson as gj
import shapely.geometry as sh
from multiprocessing import Process, Manager, Pool
from functools import partial

def make_shape(L,LinePlanningNumber):
	

if __name__ == "__main__":
	print("Start loading data")
	segments = pd.read_csv('JOPATILIXX.TMI', sep='|',usecols=['[DataOwnerCode]','[LinePlanningNumber]','[TimingLinkOrder]','[UserStopCodeBegin]','[UserStopCodeEnd]','[DisplayPublicLine]','[ProductFormulaType]'])
	pointsOnSegments = pd.read_csv('POOLXXXXXX.TMI', sep='|',usecols=['[UserStopCodeBegin]','[UserStopCodeEnd]','[PointCode]','[DistanceSinceStartOfLink]'])
	points = pd.read_csv('POINTXXXXX.TMI',sep='|',usecols=['[PointCode]','[LocationX_EW]','[LocationY_NS]'])
	print("Finished loading data")

	print("Joining the point on links on the segments")
	joined_segments = pd.merge(segments, pointsOnSegments, how="inner", on=['[UserStopCodeBegin]','[UserStopCodeEnd]'])
	print("Joining the points")
	points_joined_segments = pd.merge(joined_segments,points,how="inner",on='[PointCode]')
	print(points_joined_segments)

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