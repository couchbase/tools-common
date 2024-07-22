# genmetrics - Generate metrics metadata & boilerplate

Since 7.6.0 Couchbase services have been expected to describe what metrics they expose in a JSON file, which is then used to generate documentation. This tool will generate both the boilerplate and the JSON file from another JSON file. This input JSON file is the same as the metadata file, but with the additional `histogram_buckets` field.

## Usage

```sh
$ genmetrics <input-file> <code-output-file> <json-output-file>
```

- `input-file`: the path to a JSON file containing the metrics info
- `code-output-file`: the path to the file where the generated Go code will be written
- `json-output-file`: the path to the file where the metrics_metadata.json file will be written
