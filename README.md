Benchmarks for XTDB 2, XTDB 1, Postgres, and Sqlite using queries and anonymized data from Yakread.

## Setup

1. Download the zipfile chunks (about 9GB total)
2. Re-assemble with `cat yakread_benchmark_input.zip.part-* > input.zip`
3. Unzip and rename to `./import/`
4. Start up a postgres docker container with `./postgres.sh`
5. Run `./run.sh setup` (this might take several hours and takes up about 150GB of disk space)

## Running the benchmarks

1. Run `./run.sh benchmark`. This prints tufte output to the console and writes it to
   `results/*.edn`
2. Run `clj -M:run report` to get a nice table comparing the p50 run times for each test across DBs.
   Reads from `results/*.edn`.

## Notes

- The `:user/timezone` attribute didn't get serialized correctly by nippy, so the data ingest code
  removes it. It might already be removed from the anonymized input data.

- There is a half-baked `datomic.clj` file that I was using to set up benchmarks for Datomic Local.
  However data ingestion gradually got slower and slower, and after ~10 hours it had slowed to a
  crawl with still ~1 million entities left to go, so I gave up. Maybe an actual Datomic Pro
  instance would fare better.

- the xtdb2 and sqlite benchmarks remap document IDs: sqlite changes them from uuids to ints since
  sqlite doesn't have a native uuid type (though maybe blobs are fine?), while the xtdb2 benchmark
  keeps them as uuids but updates the prefixes to get better locality. 

Document counts:
```clojure
{"ad_clicks"  3341,
 "ad_credits" 184,
 "ads"        89
 "bulk_sends" 829,
 "digests"    23091,
 "feeds"      24468,
 "items"      17460648,
 "skips"      4494,
 "subs"       107597,
 "user_items" 236804,
 "users"      4835}
```
