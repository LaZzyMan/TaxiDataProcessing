import geopandas as gpd
from shapely.geometry import Point


class SpatialUnit:
    def __init__(self, shp_path):
        self.gdf = gpd.GeoDataFrame.from_file(shp_path)

    def find_unit_by_point(self, lon, lat):
        '''
        :param lon:
        :param lat:
        :return: TAZID or -1(Unit Not Found)
        '''
        point = Point(lon, lat)
        self.gdf['contain'] = self.gdf.contains(point)
        result = self.gdf[su.gdf['contain'] == True].TAZID.tolist()
        if len(result) > 0:
            return result[0]
        else:
            return -1


if __name__ == '__main__':
    # test
    su = SpatialUnit('../SpatialUnit/TAZ2010.shp')
    r = su.find_unit_by_point(lon=116.50120, lat=39.87480)
    print(r)
