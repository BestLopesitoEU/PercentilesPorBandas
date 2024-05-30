import xarray as xr
import numpy as np
import os
import pandas as pd
import csv  
import requests
import gc

def get_csv_from_netcdf() :
    gc.enable()
    climate_zones ={
        "polar_north": {
            "id": 1,
            "lat_min": 65,
            "lat_max": 90,
            "percentile" : {
                "80" : "",
                "85" : "",
                "90" : "",
                "95" : "",
                "97.5" : "",
            }
        },
        "temperate_north": {
            "id": 2,
            "lat_min": 35,
            "lat_max": 65,
            "percentile" : {
                "80" : "",
                "85" : "",
                "90" : "",
                "95" : "",
                "97.5" : "",
            }
        },
        "dry_north": {
            "id": 3,
            "lat_min": 23.5,
            "lat_max": 35,
            "percentile" : {
                "80" : "",
                "85" : "",
                "90" : "",
                "95" : "",
                "97.5" : "",
            }
        },
        "tropical": {
            "id": 4,
            "lat_min": -23.5,
            "lat_max": 23.5,
            "percentile" : {
                "80" : "",
                "85" : "",
                "90" : "",
                "95" : "",
                "97.5" : "",
            }
        },
        "dry_south": {
            "id": 5,
            "lat_min": -35,
            "lat_max": -23.5,
            "percentile" : {
                "80" : "",
                "85" : "",
                "90" : "",
                "95" : "",
                "97.5" : "",
            }
        },
        "temperate_south": {
            "id": 6,
            "lat_min": -65,
            "lat_max": -35,
            "percentile" : {
                "80" : "",
                "85" : "",
                "90" : "",
                "95" : "",
                "97.5" : "",
            }
        },
        "polar_south": {
            "id": 7,
            "lat_min": -90,
            "lat_max": -65,
            "percentile" : {
                "80" : "",
                "85" : "",
                "90" : "",
                "95" : "",
                "97.5" : "",
            }
        },
    }

    with open("/home/pablo807/workspace/NetcdfClimatico/Data_Final.csv", "w", newline="") as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(["indicator_id", "Zone_Id", "Percentile", "Value"])

        indicators_ids = [10, 26, 27, 28, 29, 30, 31, 34]
        lista_percentiles = [80, 85, 90, 95, 97.5]
        for indicator_id in indicators_ids:
            indicator = requests.get("https://climatehubdev.ihcantabria.com/v1/public/open-dap/metadata?indicator-id={0}&climate-case-id=9".format(indicator_id)).json()
            path = os.path.basename(os.path.normpath(indicator[0]["climateCases"][0]["url"]))
            path = path.replace(".nc", "")
            for climate_zone in climate_zones:
                ds = xr.open_dataset(indicator[0]["climateCases"][0]["url"], engine="netcdf4", decode_cf=False)
                try: 
                    ds = ds.rename({"lat" : "latitude", "lon" : "longitude"})
                except:
                    pass
                if indicator_id != 30:
                    ds = ds.dropna(dim="latitude", how="all")
                else:
                    pass
                print(ds.dims)
                mask_lon = (ds.longitude >= -180) & (ds.longitude <= 180)
                mask_lat = (ds.latitude >= climate_zones[climate_zone]["lat_min"]) & (ds.latitude <= climate_zones[climate_zone]["lat_max"])
                if mask_lat.any() == True:
                    ds = ds.where(mask_lon & mask_lat, drop=True)
                    value_percentile = np.nanpercentile(ds.variables[path], lista_percentiles)
                    climate_zones[climate_zone]["percentile"]["80"] = value_percentile[0]
                    climate_zones[climate_zone]["percentile"]["85"] = value_percentile[1]
                    climate_zones[climate_zone]["percentile"]["90"] = value_percentile[2]
                    climate_zones[climate_zone]["percentile"]["95"] = value_percentile[3]
                    climate_zones[climate_zone]["percentile"]["97.5"] = value_percentile[4]
                    for percentil in lista_percentiles: 
                        writer.writerow([indicator_id,
                                        climate_zones[climate_zone]["id"],
                                        percentil,
                                        climate_zones[climate_zone]["percentile"][f"{percentil}"]])
                else:
                    for percentil in lista_percentiles: 
                        writer.writerow([indicator_id,
                                        climate_zones[climate_zone]["id"],
                                        percentil,
                                        "NaN"])

                
    

get_csv_from_netcdf()
