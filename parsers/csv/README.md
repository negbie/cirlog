Example CLI usage (from cirlog root)
```
cirlog -p csv \
  -f some/path/system.log \
  --dataset 'MY_TEST_DATASET' \
  --backfill \
  --csv.fields="time,field_1,field_2,field_3"
  --csv.timefield="time" \
  --csv.time_format="%H:%M:%S"
```
