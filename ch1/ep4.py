import pandas as pd

route_types = {
    "airline": "string",
    "src": "string",
    "dest": "string",
    "codeshare": "string",
    "stops": "float",
    "equipment": "string"
}

airline_types = {
    "airport_id": "float",
    "name": "string",
    "alias": "string",
    "iata": "string",
    "icao": "string",
    "callsign": "string",
    "country": "string",
    "active": "string"
}

routes_df = pd.read_csv("./data/input/ch1/ep4/routes-raw.csv", dtype=route_types)

airline_df = pd.read_csv("./data/input/ch1/ep1/deb-airlines.csv", dtype=airline_types, header=0, index_col=0)

airport_df = pd.read_parquet("./data/input/ch1/ep4/deb-airports.parquet")

# get all unique IATA codes from airport data
airport_iata = airport_df.iata.unique()
airline_iata = airline_df.iata.unique()
airline_icao = airline_df.icao.unique()

filtered_df = routes_df[routes_df.airline.isin(airline_iata) | routes_df.airline.isin(airline_icao)]
valid_routes = filtered_df[filtered_df.src.isin(airport_iata) & filtered_df.dest.isin(airport_iata)]
#  get all unique IATA and ICAO columns
#  filter route dataframe to only show routes where 
# routes.IATA matches either unique airport IATA or airline IATA/ICAO 

valid_routes.to_parquet("deb-routes.parquet")


print(routes_df.size)
print(valid_routes.size)