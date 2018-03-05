import os
import requests
from datetime import datetime
from requests.auth import HTTPBasicAuth
from collections import deque
import json, io
from zipfile import ZipFile
import matplotlib.pyplot as plt
from time import sleep
from multiprocessing import Pool
import rasterio, numpy
import tempfile
from xml.dom import minidom
from matplotlib.colors import LinearSegmentedColormap
from glob import glob
import time

def progress(label, current, total, unit="bytes"):
    per = "#" * int(current / int(total) * 50)
    if current == total - 1:
        print("\r {}: [{:50}] {:,}/{:,}".format(label, "#"*50, current+1, total), end="")
        return
    print("\r {}: [{:50}] {:,}/{:,}".format(label, per, current, total), end="")
    # print("\r {}: [{:50}] {:,}/{:,} {}".format(label, per, current, total, unit), end="")

class PlanetClient(object):
    """docstring for PlanetClient."""

    base_url = "https://api.planet.com"
    sess = requests.Session()
    sess.headers.update({'content-type': 'application/json'})
    filters = {}
    search_result = []
    items = []
    assets = []
    satellite = "PSScene3Band"
    pending_clips = {}
    downloaded_clips = []
    clip_q = deque()
    pending_assets = deque()
    ready_assets = deque()
    invalid_assets = deque()
    static_path = './ndvis/'
    color_map = LinearSegmentedColormap.from_list('my_list', [  (.9, 0.2, 0), (1, 1, 0),  (0.4, .9, 0),], N=10000)
    issue = ""
    registery = []
    """
        # the geo json geometry object we got from geojson.io
        geo_json_geometry = {
          "type": "Polygon",
          "coordinates": [ [1, 1], [0, 0], [0, 1], [1, 0] ]
        }

        # filter for items the overlap with our chosen geometry
        geometry_filter = {
          "type": "GeometryFilter",
          "field_name": "geometry",
          "config": geo_json_geometry
        }

        # create a filter that combines our geo and date filters
        # could also use an "OrFilter"
        redding_reservoir = {
          "type": "AndFilter",
          "config": [geometry_filter, date_range_filter, cloud_cover_filter]
        }

        # Stats API request object
        stats_endpoint_request = {
          "interval": "day",
          "item_types": satellite,
          "filter": redding_reservoir
        }

        # setup auth
        session = requests.Session()
        session.auth = ('token', '')
        """

    def __init__(self, token="", issue=""):
        self.sess.auth = (token, '')
        print(self.sess.auth)
        if not os.path.exists(self.static_path+issue):
            os.makedirs(self.static_path+issue)
        self.issue = issue
        self.registery = glob(self.static_path+issue+"/*.npy")

    def getSatellites(self):
        item_types_uri = "/data/v1/item-types/"
        res = self.sess.get(self.base_url + item_types_uri)
        self.satellites = [sat['id'] for sat in res.json()['item_types']]
        return self.satellites

    def getItem(self, item_id):
        get_uri = r"/data/v1/item-types/{}/items/{}"
        resp = self.sess.get(
            self.base_url + get_uri.format(self.satellite, item_id))

        if resp.ok:
            return resp.json()
        else:
            raise Exception(resp.reason)

    def getItems(self):
        # TODO: parallel async fetch
        """
        for item in self.search_result:
            yield self.getItem(item['id'])
        """
        # assert self.search_result, "Search result is empty"
        self.search_result.clear()
        search = self.quickSearch()
        self.items.clear()
        print('')
        for i, item in enumerate(search):
            self.items.append(self.getItem(item['id']))
            progress("Fetching items", i, len(search), 'items')

        print('')
        json.dump(self.items, open(self.static_path+self.issue+"/"+datetime.now().strftime("%Y_%m%d")+"_items.json", "w"))
        return self.items

    def getAssetsOf(self, item):
        """
            Gets the assets of the target item
        """
        resp = self.sess.get(item['_links']['assets'])
        if resp.ok:
            return resp.json()


    def getAssets(self):
        # TODO: parallel async fetch
        """
            Gets assets of all items
        """
        # TODO: hard to test
        # for item in self.items:
        #     yield self.getAssetsOf(self.getItem(item['id']))
        # assert self.items, "No items found!"

        for i, item in enumerate(self.items or self.getItems()):
            asset = self.getAssetsOf(item)

            if 'analytic' in asset.keys():
                item['asset'] = asset

            progress("Fetching assets", i, len(self.items), 'assets')
        return self.items

    def quickSearch(self):
        # Stats API request object
        stats_endpoint_request = {
            "interval": "day",
            "item_types": [self.satellite],
            "filter": {
                "type": "AndFilter",
                "config": list(self.filters.values())
            }
        }
        # fire off the POST request
        result = self.sess.post('https://api.planet.com/data/v1/quick-search',
                                json=stats_endpoint_request)

        if result.ok:
            self.search_result = result.json()['features']
            json.dump(self.search_result, open(self.static_path+self.issue+"/"+datetime.now().strftime("%Y_%m%d")+"_sr.json", "w"))
        else:
            raise Exception(result.reason)
        return self.search_result

    def getClip(self, item, geom=None, key='analytic'):
        clip_uri = "/compute/ops/clips/v1/"
        # if self.static_path+self.issue+"/"+item['id']+"_ndvi_.npy" in self.registery:
        #     print("Passing", item['id'])
        #     return
        geom = geom if geom else self.filters['geometry']['config']['coordinates']
        if key == "visual":
            satellite = "PSScene3Band"
        else:
            satellite = self.satellite
        data = {
                "aoi": {
                    "type": "Polygon",
                    "coordinates": geom,
                },
                "targets": [{
                    "item_id": item['id'],
                    "item_type":  satellite, #"PSScene3Band",
                    "asset_type": key
                }]
            }
        ac_resp =  self.sess.post(self.base_url + clip_uri,
                                  json=data,
                                  headers={'content-type': 'application/json'})

        if ac_resp.ok:
            clip = ac_resp.json()
            clip["item_id"] = item['id']
            return clip
        else:
            print(ac_resp.reason)


    def getN(self, clip):
        print("\r Activating:", clip['item_id'], end="")
        _clip = self.sess.get(clip['_links']['_self'])

        if _clip.reason == "Too Many Requests":
            return clip
        _clip = _clip.json()
        _clip.update({ "item_id": clip['item_id'] })
        return _clip


    def clipAll(self):
        """
        TODO: activate analytic & visual clips at the same time
        """
        analytics = []
        visuals= []
        for i, item in enumerate(self.items):
            analytics.append(self.getClip(item, key="analytic"))
            progress("Fetching   items", i, len(self.items), "")
            visuals.append(self.getClip(item, key="visual"))
            progress("Fetching visuals", i, len(self.items), "")
        print("")
        clipQ = deque(analytics)
        visualQ = deque(visuals)
        i = 0
        while clipQ:
            next_clip = clipQ.popleft()
            if next_clip:
                if next_clip["state"] != "succeeded":
                    clipQ.append(self.getN(next_clip))
                    i+=1
                else:
                    print("\n\tDownloading")
                    r = self.sess.get(next_clip['_links']['results'][0])
                    print("\tExtracting")
                    try:
                        c = self.extract(r, file_name=self.static_path+self.issue+"/"+next_clip['item_id']+".zip")
                        c.update({ "item_id": next_clip['item_id'] })
                        # c2 = c.copy()

                        self.generateNDVI(c),
                    except:
                        continue
                    # self.generateGSAVI(c)

                if len(clipQ):
                    sleep(1/len(clipQ))
        while visualQ:
            next_visual = visualQ.popleft()
            if next_visual:
                if next_visual["state"] != "succeeded":
                    visualQ.append(self.getN(next_visual))
                else:
                    print("\n\tDownloading")
                    r = self.sess.get(next_visual['_links']['results'][0])
                    # c = self.extract(r, file_name=self.static_path+self.issue+"/"+next_visual['item_id']+"_rgb_.zip")
                    #self.generateNDVI(c)
                    print("\tExtracting")
                    z = ZipFile(io.BytesIO(r.content))
                    a = z.read([x for x in z.filelist if x.filename.endswith(".tif")][-1])
                    open(self.static_path+self.issue+"/"+next_visual['item_id']+"_rgb_.tif", "wb").write(a)
                    c.update({ "item_id": next_visual['item_id'] })
                if len(visualQ):
                    sleep(1/len(visualQ))

    def extract(self, resp, file_name=None):
      cont = resp.content
      if file_name:
          open(file_name, "wb").write(cont)
      z = ZipFile(io.BytesIO(cont))
      return { x.filename.split('.')[1]: z.open(x) for x in z.filelist if 'udm' not in x.filename }

    def activate(self, item, only=['analytic', 'analytic_xml']):
        ar = {}
        status = ""
        for pname, prop in item['asset'].items():
            if only and pname not in only:
                continue
            new_p = self.sess.get(prop['_links']['_self']).json()
            if new_p['status'] != 'active':
                if prop['status'] == 'activating':
                    status = "Pending: "+pname
                    continue
                status = "Activating: "+pname
                a = self.sess.post(prop['_links']['activate'])
                item['asset'][pname] = self.sess.get(prop['_links']['_self']).json()
                status = "Updated: " + pname
            else:
                status = "Active: " + pname
            # asset[pname] = self.sess.post(prop['_links']['_self']).json()
        # asset = self.sess.get(asset['_links']['_self']).json()

        return status
        # self.pending_assets.append(asset)

    def activateItems(self, only='analytic'):
        print("")
        for i, item in enumerate(self.items):
            print("\nActivating: ", item['id'])
            # for asst in item['asset']
            status = self.activate(item)
            progress(status, i, len(self.items), 'items')
                #
    def downloadVisual(self, asset):
        if asset['visual']['status'] != 'active':
            return
        print("\tDownloading: Visual of the", asset)
        res = self.sess.get(asset['visual']['location'])
        return res.content

    def get(self, *args, **kwargs):
        return self.sess.get(*args, **kwargs)

    def post(self, *args, **kwargs):
        return self.sess.post(*args, **kwargs)

    def getAnalytic(self, asset, key='analytic'):
        from xml.dom import minidom
        if asset[key]['status'] != 'active':
            return

        if asset[key+"_xml"]['status'] != 'active':
            return

        print("\tFetching Meta data:", key+"_xml")
        analytic_xml_resp = self.sess.get(asset[key+'_xml']['location'])
        print("\tFetching imagery:", key)
        analytic_resp = self.sess.get(asset[key]['location'])

        return { 'content': analytic_resp, 'xml': analytic_xml_resp }


    def extractBands(self, analytic, file_name=None):
        print("\tDisassembling necessary bands")
        tmpfile = None
        if file_name:
            tmpfile = open(file_name, "wb")
            tmpxml = open(file_name.split(".")[0]+".xml", "wb")
            tmpfile.write(analytic['tif'].read())
            tmpxml.write(analytic['xml'])
            print("writing to", file_name)

        else:
            tmpfile = tempfile.NamedTemporaryFile()
            tmpfile.write(analytic['tif'].read())

        with rasterio.open(tmpfile.name, 'r') as dataset:
            # print(dataset.profile)
            # data_array = dataset.read()
            band_blue = dataset.read(1)
            band_green = dataset.read(2)
            band_red = dataset.read(3)
            band_nir = dataset.read(4)
            profile = dataset.profile

            total = numpy.zeros(dataset.shape)
            for band in band_blue, band_green, band_red, band_nir:
                total += band


                # Write the product as a raster band to a new 8-bit file. For
                # the new file's profile, we start with the meta attributes of
                # the source file, but then change the band count to 1, set the
                # dtype to uint8, and specify LZW compression.
                profile = dataset.profile
                # profile.update(dtype=rasterio.uint8, count=1, compress='lzw')

        return band_blue, band_green, band_red, band_nir, total, profile


    def calculateCoefficients(self, analytic):
        xmldoc = minidom.parseString(analytic['xml'].read())
        nodes = xmldoc.getElementsByTagName("ps:bandSpecificMetadata")

        # data = self.downloadVisual(asset)
        # XML parser refers to bands by numbers 1-4
        print("\tCalculating coefficients")
        coeffs = {}
        for node in nodes:
            bn = node.getElementsByTagName("ps:bandNumber")[0].firstChild.data
            if bn in ['1', '2', '3', '4']:
                i = int(bn)
                value = node.getElementsByTagName("ps:reflectanceCoefficient")[0].firstChild.data
                coeffs[i] = float(value)
        return coeffs

    def generateNDVI(self, analytic, key='analytic'):

        self.static_path+self.issue+"/"+analytic['item_id']+"_ndvi_.npy"
        print("\tGenerating NDVI")

        coeffs = self.calculateCoefficients(analytic)

        band_blue, band_green, band_red, band_nir, real, profile = self.extractBands(analytic)
        print("\tSaving imagery:", self.static_path+self.issue+"/"+analytic['item_id']+".tif")

        # with rasterio.open(self.static_path+self.issue+"/"+analytic['item_id']+".tif", 'w', **profile) as dst:
        #     dst.write(real.astype(rasterio.uint16), 1)
        band_blue = band_blue * coeffs[1]
        band_green = band_green * coeffs[2]
        band_red = band_red * coeffs[3]
        band_nir = band_nir * coeffs[4]

        # Allow division by zero
        numpy.seterr(divide='ignore', invalid='ignore')

        # Calculate NDVI
        print("\tCalculating NDVI")
        # avarage ndvireason
        # (NIR + Green) - (2*Blue) / ((NIR + Green) + 2*Blue)
        L = 0
        # ndvi =   ((band_nir.astype(float) - band_red.astype(float)) / (band_nir.astype(float) + band_red.astype(float) ))
        # ndvi =   ((band_nir.astype(float) - real.astype(float)) / (band_nir.astype(float) + real.astype(float) ))
        #
        # Chlorophyll Vegetation Index: CVI = (NIR * Red) / (Green ^ 2)
        ndvi =   ((band_nir.astype(float) * band_red.astype(float)) / (band_green.astype(float)**2 ))

        #ndvi = (
        # (band_nir.astype(float) + band_green.astype(float)) - 2*band_blue.astype(float) / (band_nir.astype(float) + band_green.astype(float)) + 2*band_blue.astype(float)
        # )

        # gndvi = ((band_nir.astype(float) - band_green.astype(float)) / (band_nir.astype(float) + band_green.astype(float)))
        # gipvi = ((band_nir.astype(float)) / (band_nir.astype(float) + band_green.astype(float)))

        print("\tNDVI Mean:", numpy.nanmean(ndvi))
        # Set spatial characteristics of the output object to mirror the input

        print("\tSaving NDVI", self.static_path+self.issue+"/"+analytic['item_id']+"_ndvi.npy")
        numpy.save(self.static_path+self.issue+"/"+analytic['item_id']+"_ndvi_.npy", ndvi)
        # Create the file
        # with rasterio.open('ndvi.tif', 'w', **kwargs) as dst:
        #     dst.write_band(1, ndvi.astype(rasterio.float32))
        print("\tSaving NDVI imagery", self.static_path+self.issue+"/"+analytic['item_id']+"_ndvi_colormap.png")
        plt.imsave(self.static_path+self.issue+"/"+analytic['item_id']+"_ndvi_"+str(L)+"_ndvi.png", ndvi, cmap=self.color_map)


    def generateGSAVI(self, analytic, key='analytic'):

        self.static_path+self.issue+"/"+analytic['item_id']+"_gsavi_.npy"
        print("\tGenerating GSAVI")

        coeffs = self.calculateCoefficients(analytic)

        band_green, band_red, band_nir, real, profile = self.extractBands(analytic)
        print("\tSaving imagery:", self.static_path+self.issue+"/"+analytic['item_id']+".tif")

        with rasterio.open(self.static_path+self.issue+"/"+analytic['item_id']+".tif", 'w', **profile) as dst:
            dst.write(real.astype(rasterio.uint16), 1)

        band_green = band_green * coeffs[2]
        band_red = band_red * coeffs[3]
        band_nir = band_nir * coeffs[4]

        # Allow division by zero
        numpy.seterr(divide='ignore', invalid='ignore')

        # Calculate GSAVI
        print("\tCalculating GSAVI")
        # avarage ndvireason
        # [(NIR – Green) / (NIR + Green +L)] * (1 + L), where L = 0.5
        gsavi = 1.5*((band_nir.astype(float) - band_green.astype(float))/(band_nir.astype(float) + band_green.astype(float) + 0.5))
        # 1.5 * ((band_nir.astype(float) - band_red.astype(float)) / (band_nir + band_red ))

        print("\GSAVI Mean:", numpy.nanmean(gsavi))
        # Set spatial characteristics of the output object to mirror the input

        print("\tSaving GSAVI", self.static_path+self.issue+"/"+analytic['item_id']+"_gsavi.npy")
        numpy.save(self.static_path+self.issue+"/"+analytic['item_id']+"_gsavi_.npy", gsavi)
        # Create the file
        # with rasterio.open('ndvi.tif', 'w', **kwargs) as dst:
        #     dst.write_band(1, ndvi.astype(rasterio.float32))
        print("\tSaving GSAVI imagery", self.static_path+self.issue+"/"+analytic['item_id']+"_gsavi_colormap.png")
        plt.imsave(self.static_path+self.issue+"/"+analytic['item_id']+"_gsavi.png", gsavi, cmap=self.color_map)


    @property
    def geometry(self):
        assert 'geometry' in self.filters, "Geometry not set yet!"
        return self.filters['geometry']

    @geometry.setter
    def geometry(self, geom):
        """

        planet_client =  PlanetClient()

        # create a 2D array that represents
        # define endpoints of the polygon shape
        geom = [
            [ 0, 0 ],
            [ 0, 1 ],
            [ 1, 0 ],
            [ 1, 1 ]
        ]

        # set geometry - apply the geometry to the api filter
        planet_client.geometry = geom

        """
        self.geom = {
            "type": "Polygon",
            "coordinates": geom
        }

        self.filters['geometry'] = {
            "type": "GeometryFilter",
            "field_name": "geometry",
            "config": self.geom
        }
        print("Set geometry", self.geom)

    @property
    def cloudCover(self):
        assert 'cloud_cover' in self.filters, "Cloud cover not set yet!"
        return self.filters['cloud_cover']

    @cloudCover.setter
    def cloudCover(self, ratio):
        """

        planet_client =  PlanetClient()

        # must be between 0 and 1
        cloud_cover = 0.1

        # set cloud cover - apply given cloud ration to the api filter
        planet_client.cloudCover = cloud_cover


        # filter any images which are less than 10% clouds
        cloud_cover_filter = {
          "type": "RangeFilter",
          "field_name": "cloud_cover",
          "config": {
            "lte": 0.1
          }
        }
        """
        assert type(ratio) in (
            float, int), "Unsupported data type, Only float and int!"
        assert 0 <= ratio <= 1, "Cloud cover value must be between 0 and 1!"

        self.filters['cloud_cover'] = {
            "type": "RangeFilter",
            "field_name": "cloud_cover",
            "config": {
                "lte": ratio
            }
        }
        print("Set cloud cover ratio:", self.filters['cloud_cover']['config']['lte'])

    @property
    def dateRange(self):
        assert 'acquired' in self.filters, "Date range not set yet!"
        return self.filters['acquired']


    @dateRange.setter
    def dateRange(self, range_):
        """

        planet_client =  PlanetClient()

        # define a range as
        # list, tuple
        date_range = [
            datetime(2016, 1, 1, 0, 0),
            datetime(2017, 1, 1, 0, 0)
        ]

        # or dict
        date_range = {
            "from": datetime(2016, 1, 1, 0, 0),
            "to": datetime(2017, 1, 1, 0, 0)
        }

        # set range - apply given range to the api filter
        planet_client.dateRange = date_range

        # filter images acquired in a certain date range

        date_range_filter = {
          "type": "DateRangeFilter",
          "field_name": "acquired",
          "config": {
            "gte": "2016-07-01T00:00:00.000Z",
            "lte": "2018-11-13T00:00:00.000Z"
          }
        }
        """
        if type(range_) is dict:
            range_ = (range_['from'], range_['to'])

        assert all(map(lambda x: type(x) is datetime, range_)), "Only the datetime objects are accepted!"
        range_dict = {
          "gte": range_[0].isoformat() + "Z",
          "lte": range_[1].isoformat() + "Z"
        }

        self.filters['acquired'] = {
            "type": "DateRangeFilter",
            "field_name": "acquired",
            "config": range_dict
        }

        print("Set date range:", list(self.filters['acquired']['config'].values()))

### You dont have to use these functions, these are just defined to automate the process #######
def calculate(geom, min_date, max_date, cloud_cover, issue="", token=""):
    planet_client = PlanetClient(token=token, issue=issue+"/"+geom['plant_type'])
    planet_client.satellite = "PSScene4Band"

    if not os.path.exists(planet_client.static_path+issue+"/"+geom['plant_type']):
        os.makedirs(planet_client.static_path+issue+"/"+geom['plant_type'])
    planet_client.issue = issue+"/"+geom['plant_type']
    print("Created folder:", planet_client.static_path+issue+"/"+geom['plant_type'])
    planet_client.dateRange =  [
        max_date,
        min_date,
    ]
    planet_client.cloudCover = cloud_cover
    planet_client.geometry = geom['geometry']
    planet_client.quickSearch()
    print("Searching")
    planet_client.getItems()
    print("Getting")
    planet_client.clipAll()

def crawl(gjson, min_date, max_date, cloud_cover, issue="", token=""):
    """
    usage:
        output = "cenk_bey"
        token = "your_token"

        date_min = datetime(2017, 11, 22)
        date_max = datetime.now()
        cloud_cover = .1
        geometry = "cenk.geojson" # you want to make sure you define the plant type in geojson properties like ..., "properties": { "type": "rice" }, ...

        crawl(geometry, date_max, date_min, cloud_cover, issue=output, token=token)
    """
    if type(gjson) == str:
        try:
            gjson = json.loads(open(gjson).read())
        except:
            raise Exception("File not found", gjson)
        geoms = [ {"plant_type": x['properties']['type'], "geometry": x['geometry']['coordinates']} for x in gjson['features'] if x['geometry']['type'] == "Polygon" ]
        for geom in geoms:
            calculate(geom, min_date, max_date, cloud_cover, issue=issue, token=token)

#################################################################################################

if __name__ == '__main__':

    # date_min = datetime(2017, 11, 22)
    # date_max = datetime.now()
    # cloud_cover = .1
    # geometry = "cenk.geojson"
    # output = "cenk_bey"
    # token = "your_token"
    # crawl(geometry, date_max, date_min, cloud_cover, issue=output, token=token)

    geofile = "basic.geojson"
    token = os.environ["PL_API_KEY"] or "your_token"
    date_min = datetime(2018, 1, 10)
    date_max = datetime.now()
    cloud_cover = .1
    geom = json.load(open(geofile))

    issue = "output"

    planet_client = PlanetClient(token=token, issue=issue)

    # initialize filters
    planet_client.dateRange =  { "from": date_min, "to": date_max }
    planet_client.satellite = "PSScene4Band"
    planet_client.cloudCover = cloud_cover
    planet_client.geometry = geom['features'][0]['geometry']['coordinates']
    # planet_client.static_path = "main_folder"  default is ./ndvis
    # these methods returns and also stores their results
    planet_client.quickSearch()
    planet_client.getItems()
    t = time.time()

    # if your geomery is too complex, this process can take a really long time
    # (2 items with 4 edges takes around 150 seconds to activate & download)
    # if the amount of objects in activation query increases, frequency of sending requests speeds up.
    # planet's activation process is the bottleneck
    planet_client.clipAll()
