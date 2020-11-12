import pandas as pd

airport_types = {
    "iata":"string",
    "airport": "string",
    "city":"string",
    "state":"string",
    "country":"string",
    "lat":"float",
    "long":"float64"
}

asc_df = pd.read_csv('ASC-airports-raw.csv', delimiter=",", dtype=airport_types)
# asc_df.info()

of_names = ["id", "name", "city", "country", "iata", "icao", "lat", "long", "altitude", "timezone_utc", "dst", "tz", "type", "source"]
of_types = {"id":"int", "name":"string", "city":"string", "country":"string", "iata":"string", "icao":"string", "lat":"float64", "long":"float64", "altitude":"float64", "timezone_utc":"string", "dst":"string", "tz":"string", "type":"string", "source":"string"}
of_df = pd.read_csv("OF-airports-raw.csv", delimiter=",", index_col=0, names=of_names, dtype=of_types)
# of_df.info()
# of_df.head()

#  merge dataframes
asc_of_df = pd.merge(asc_df, of_df, how="left", on="iata")
asc_of_df.drop(axis=1, columns=['airport', 'city_x', 'country_x', 'lat_y', 'long_y', 'type', 'source'], inplace=True)
asc_of_df.columns = ['iata', 'state', 'lat', 'long', 'name', 'city', 'country', 'icao', 'altitude', 'utc_offset', 'dst', 'tz']
asc_of_df = asc_of_df[['iata', 'name', 'city', 'state', 'country', 'icao', 'lat', 'long', 'altitude', 'utc_offset', 'dst', 'tz']]
# asc_of_df.info()

usa_df = asc_of_df[asc_of_df.country == "United States"]
usa_df.head()
usa_df.info()

usa_df.to_parquet('.dev-airports.parquet')