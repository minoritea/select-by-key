# select-by-key

A command that divides input into keys and values,
and returns the values corresponding to the keys selected in the pipeline.

## How to install

```bash
go install github.com/minoritea/select-by-key
```

## How to use

```bash
echo "key value" | select-by-key -- grep key #> value
```

1. Pass delimiter-separated key-value pairs or JSON of an object to the standard input.
  - If `-json` option is given, the input will be interpreted as a JSON string.
  - Otherwise, the input will be divided into keys and values by the first delimiter for each lines. 
  - The delimiter is specified with the `-d` option and defaults to space.
2. Pass a filter command and arguments after the "--".
  - The filter command must take keys and output the selected keys.
3. Then `select-by-key` will output the values corresponding to the selected keys.

## License

MIT License(see LICENSE file).
