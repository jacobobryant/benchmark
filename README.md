Input data:

1. Download the zipfile chunks (about 9GB total)
2. Re-assemble with `cat yakread_benchmark_input.zip.part-* > input.zip`
3. Unzip and rename to `./import/`

Setup:

1. Start up a postgres docker container with `./postgres.sh` (only if you want to run the benchmarks
   against postgres)
2. Run `./setup.sh` (this might take over an hour and could use up >100GB of disk space)

Benchmarks:

1. Run `for alias in xtdb2 xtdb1 sqlite postgres; do clj -M:$alias benchmark; done`

Notes:

- The `:user/timezone` attribute didn't get serialized correctly by nippy, so the data ingest code
  removes it. It might already be removed from the anonymized input data.
- There is a half-baked `datomic.clj` file that I was using to set up benchmarks for Datomic Local.
  However data ingestion gradually got slower and slower, and after ~10 hours it had slowed to a
  crawl with still ~1 million entities left to go, so I gave up. Maybe an actual Datomic Pro
  instance would fare better.


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
