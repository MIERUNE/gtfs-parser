# gtfs_parser

## LICENSE

MIT License

## Installation

```sh
pip install gtfs-parser
```

## API

```python
import gtfs_parser

# construct GTFS object
gtfs = gtfs_parser.GTFSFactory(zip_path)

# parse as GeoJSON
stops = gtfs_parser.parse.read_stops(gtfs)
routes = gtfs_parser.parse.read_routes(gtfs)

# aggregate frequency
aggregator = gtfs_parser.aggregate.Aggregator(gtfs, yyyymmdd=yyyymmdd)
interpolated_stops = aggregator.read_interpolated_stops()
route_freq = aggregator.read_route_frequency()
```

## CLI

```
usage: gtfs-parser [-h] [--parse_ignoreshapes] [--parse_ignorenoroute]
                   [--aggregate_yyyymmdd AGGREGATE_YYYYMMDD]
                   [--aggregate_nounifystops]
                   [--aggregate_delimiter AGGREGATE_DELIMITER]
                   [--aggregate_begintime AGGREGATE_BEGINTIME]
                   [--aggregate_endtime AGGREGATE_ENDTIME]
                   mode src dst

positional arguments:
  mode
  src
  dst

optional arguments:
  -h, --help            show this help message and exit
  --parse_ignoreshapes
  --parse_ignorenoroute
  --aggregate_yyyymmdd AGGREGATE_YYYYMMDD
  --aggregate_nounifystops
  --aggregate_delimiter AGGREGATE_DELIMITER
  --aggregate_begintime AGGREGATE_BEGINTIME
  --aggregate_endtime AGGREGATE_ENDTIME
```

### Example

```sh
gtfs-parser parse gtfs.zip output
gtfs-parser parse gtfs_dir output --parse_ignoreshapes
gtfs-parser aggregate gtfs.zip output
gtfs-parser aggregate gtfs_dir output --aggregate_nounifystops
```

## Authors

- Kanahiro Iguchi ([@Kanahiro](https://github.com/Kanahiro)) - original author
- Kohei Ota ([@takohei](https://github.com/takohei))
