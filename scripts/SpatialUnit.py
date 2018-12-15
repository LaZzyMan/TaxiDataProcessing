import geopandas as gpd
from shapely.geometry import Point
import time


class SpatialUnit:
    def __init__(self, shp_path):
        self.gdf = gpd.GeoDataFrame.from_file(shp_path)
        self.boundary = self.gdf.total_bounds

    def find_unit_by_point(self, lon, lat):
        '''
        :param lon:
        :param lat:
        :return: TAZID or -1(Unit Not Found)
        '''
        if lon < self.boundary[0] or lon > self.boundary[2]:
            return -1
        if lat < self.boundary[1] or lat > self.boundary[3]:
            return -1
        point = Point(lon, lat)
        self.gdf['contain'] = self.gdf.contains(point)
        result = self.gdf[self.gdf['contain'] == True].TAZID.tolist()
        if len(result) > 0:
            return result[0]
        else:
            return -1


if __name__ == '__main__':
    # test
    start = time.time()
    su = SpatialUnit('../test/TaxiData/SpatialUnit/TAZ2010.shp')
    r = su.find_unit_by_point(lon=116.50120, lat=39.87480)
    print(r)
    print(time.time() - start)
