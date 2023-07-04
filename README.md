# gtfs_parser

## LICENSE

MIT License

## Installation

```sh
pip install gtfs-parser
```

## API

TODO

## CLI

```
usage: __main__.py [-h] [--parse_ignoreshapes] [--parse_ignorenoroute]
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
python -m gtfs-parser parse gtfs.zip output
python -m gtfs-parser parse gtfs_dir output --parse_ignoreshapes
python -m gtfs-parser aggregate gtfs.zip output
python -m gtfs-parser aggregate gtfs_dir output --aggregate_nounifystops
```