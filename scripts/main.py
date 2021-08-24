import numpy as np
import pdal
import json
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, Point
from fileHandler import FileHandler

class Main:
   
    
    """
      this class perform the following Functionalities 
            => feteching
            => manipulating and 
            => visualization
    """

    def __init__(self, public_data_url: str = "https://s3-us-west-2.amazonaws.com/usgs-lidar-public/", pipeline_json_path: str="../data/pipelineGetway.json") -> None:
        """
        instantiate the class, takes public_data_url as base path and pipeline_json_path as json file from which we instantiate the pipeline 
        
        """
        self.fileHandler = FileHandler()
        self.pipeline_json = self.fileHandler.read_json(pipeline_json_path)
        self.public_data_url = public_data_url
        self.input_epsg = 3857
        self.metadata = self.fileHandler.readCsv("../data/usgs_3dep_dataofdata.csv")
        values = {"year": 0}
        self.metadata.fillna(value=values, inplace=True)

    def getPolygonBoundaries(self, polygon: Polygon):
        """
         this method takes polygon object and returns tuple as bound string 
        """
        polygon_df = gpd.GeoDataFrame([polygon], columns=['geometry'])

        polygon_df.set_crs(epsg=self.output_epsg, inplace=True)
        polygon_df['geometry'] = polygon_df['geometry'].to_crs(epsg=self.input_epsg)
        minx, miny, maxx, maxy = polygon_df['geometry'][0].bounds

        polygon_input = 'POLYGON(('
        xcords, ycords = polygon_df['geometry'][0].exterior.coords.xy
        for x, y in zip(list(xcords), list(ycords)):
            polygon_input += f'{x} {y}, '
        polygon_input = polygon_input[:-2]
        polygon_input += '))'

        return f"({[minx, maxx]},{[miny,maxy]})", polygon_input

    def getPipeline(self, region: str, polygon: Polygon):
        """
        fills empty values in the pipeline dictionary and create pdal pipeline object.
        takes region as str and polygon as Polygon object  
        and Returns:
            pdal.Pipeline: pdal pipeline object
        """
        boundaries, polygon_input = self.getPolygonBoundaries(polygon)

        full_dataset_path = f"{self.public_data_url}{region}/ept.json"

        self.pipeline_json['pipeline'][0]['filename'] = full_dataset_path
        self.pipeline_json['pipeline'][0]['bounds'] = boundaries
        self.pipeline_json['pipeline'][1]['polygon'] = polygon_input
        self.pipeline_json['pipeline'][3]['out_srs'] = f'EPSG:{self.output_epsg}'

        pipeline = pdal.Pipeline(json.dumps(self.pipeline_json))

        return pipeline

    def runPipeline(self, polygon: Polygon, epsg, region: str = "IA_FullState"):
        """
        This method runs a pdal pipeline and fetches data.
        """
        self.output_epsg = epsg
        pipeline = self.getPipeline(region, polygon)

        try:
            pipeline.execute()
            print('Pipeline executed successfully.')
            return pipeline
        except RuntimeError as e:
            print('Pipeline execution failed')
            print(e)

    def makeGeoDf(self, arr: dict):
        """
         creates a geopandas dataframe from a dictionary.
         takes arr dictionary returns geopandas data frame
         
         where X & Y are geometry data points and point Z is elevation 
        """
        geometry_points = [Point(x, y) for x, y in zip(arr["X"], arr["Y"])]
        elevetions = arr["Z"]
        df = gpd.GeoDataFrame(columns=["elevation", "geometry"])
        df['elevation'] = elevetions
        df['geometry'] = geometry_points
        df = df.set_geometry("geometry")
        df.set_crs(self.output_epsg, inplace=True)
        return df

    def getRegions(self, polygon: Polygon, epsg: int) -> list:
        """
        This method fetches all the region filenames that contain the polygon.
        Args:
            polygon (Polygon): [a polygon object]
            epsg (int): [the desired coordinate reference system(CRS)]
        Returns:
            [list]: [list of all the region filenames that contain the polygon]
        """
        self.output_epsg = epsg
        polygon_df = gpd.GeoDataFrame([polygon], columns=['geometry'])

        polygon_df.set_crs(epsg=self.output_epsg, inplace=True)
        polygon_df['geometry'] = polygon_df['geometry'].to_crs(epsg=self.input_epsg)
        minx, miny, maxx, maxy = polygon_df['geometry'][0].bounds

        cond_xmin = self.metadata.xmin <= minx
        cond_xmax = self.metadata.xmax >= maxx
        cond_ymin = self.metadata.ymin <= miny
        cond_ymax = self.metadata.ymax >= maxy

        df = self.metadata[cond_xmin & cond_xmax & cond_ymin & cond_ymax]
        sort_df = df.sort_values(by=['year'])
        regions = sort_df['filename'].to_list()
        return regions

    def getRegionData(self, polygon: Polygon, epsg: int, region: str):
        """
         fetches data for a specific region.
         polygon as Polygon object, epsg coordinate reference and region the region where the data is extracted 
         returns geopandas dataframe 
        """
        pipeline = self.runPipeline(polygon, epsg, region)
        arr = pipeline.arrays[0]
        return self.makeGeoDf(arr)

    def getData(self, polygon: Polygon, epsg: int) -> dict:
        """
        fetches data from all regions that contain a polygon.
        takes polygon and epsg 
        returns dictionary as specified polygon 
        """

        regions = self.getRegions(polygon, epsg)
        region_dict = {}
        for region in regions:
            year = int(self.metadata[self.metadata.filename == region].year.values[0])
            if year == 0:
                year = 'unknown'
            region_df = self.get_region_data(polygon, epsg, region)
            empty = region_df.empty
            if not empty:
                region_dict[year] = region_df

        return region_dict

    def plotTerrain3d(self, gdf: gpd.GeoDataFrame, fig_size: tuple=(12, 10), size: float=0.01):
        """
         displays points in a geodataframe as a 3d scatter plot
        """
        fig, ax = plt.subplots(1, 1, figsize=fig_size)
        ax = plt.axes(projection='3d')
        ax.scatter(gdf.geometry.x, gdf.geometry.y, gdf.elevation, s=size)
        plt.show()
