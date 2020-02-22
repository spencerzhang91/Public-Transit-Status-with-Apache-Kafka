"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


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


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic(
    "com.udacity.stations",
    value_type=Station)

# TODO: Define the output Kafka Topic
out_topic = app.topic(
    "com.udacity.staions.table",
    value_type=TransformedStation,
    partitions=1)


# TODO: Define a Faust Table
table = app.Table(
   "com.udacity.stations.table.v1",
   default=TransformedStation,  # <-- not sure if it is correct.
   partitions=1,
   changelog_topic=out_topic,
)


# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def station_event(incoming_events):
    async for event in incoming_events:
        transformed = TransformedStation(
            station_id = event.station_id,
            station_name = event.station_name,
            order = event.order,
            line = decide_color(event)
        )
        table[event.station_id] = transformed


def decide_color(event):
    assert isinstance(event, Station), "Incoming event from connector is not <Station> type."
    if event.red:
        return "red"
    elif event.blue:
        return "blue"
    elif event.green:
        return "green"
    else:
        logger.info(f"No line color for {event.station_id}")
        return None


if __name__ == "__main__":
    app.main()
