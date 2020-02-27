"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)
RED = 'red'
GREEN = 'green'
BLUE = 'blue'

# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str



app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("com.udacity.connect.stations", value_type=Station)

out_topic = app.topic("com.udacity.connect.stations.transformed", partitions=1, value_type=TransformedStation)

table = app.Table(
    "TransformedStation",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def transforms(stations):
    async for station in stations:
        line = RED if station.red else BLUE if station.blue else GREEN
        table[station.station_id] = TransformedStation(
            station_id=station.station_id, 
            station_name=station.name,
            order=station.order,
            line=line            
        )
        
        


if __name__ == "__main__":
    app.main()
