Puffgres is a [change data capture](https://en.wikipedia.org/wiki/Change_data_capture) pipeline that syncs changes in Postgres to Turbopuffer. 


## Quick Start

```
brew install puffgres
puffgres init 
```

This will take you through an interactive setup. Puffgres largely should be able to configure itself; it will add a few __puffgres tables to your database for its own maintenence, and a `puffgres` folder to whatever project you are in to specify configs. 

### Why

[pgvector is not very good at scale](https://alex-jacobs.com/posts/the-case-against-pgvector/) and there’s considerable performance hits to keeping large vectors on a main database instance. [Turbopuffer](https://youtu.be/_yb6Nw21QxA?t=597) is excellent, fast, and easy to use. 

I found myself, in several projects, mirroring data from Postgres to Turbopuffer for search. Every time I wrote clunky, bespoke logic. Usually this meant some sort of tracking row or separate table (`updated_in_turbopuffer_at`) and, effectively, polling logic to see if a row had changed on each run of a data pipeline. I kept rewriting the same batching / retry logic and figured, in the [primacy of toolmaking](https://www.youtube.com/watch?v=_GpBkplsGus) tradition, that I should build a more generic tool.

## Fundamentals


Puffgres has two primary surfaces, the **CLI**, for configuring one’s puffgres setup and a **runner** service that mirrors changes. You run the CLI for dev setup and the runner wherever your main services live. 

The two primitives in Puffgres are:
- **migrations**, much like a regular database. These are structured as .toml files, and are immutable. I felt this was the best solution for configurations, to indicate that changes DO NOT by default apply retroactively
- **transforms**, a Typescript API for specifying how rows are changed before they are upserted to Turbopuffer. I did this because I found I often was not simply upserting (or even) embedding rows before they went up. Sometimes I would combine two columns in the text I embedded, add some sort of prompt or guidance before embedding, truncate it (based on tokenization), or use nonstandard embedding models. Leaving these as (highly flexible) code makes it easy to maintain these.

## Acknowledgements

This project was inspired by reading Martin Kleppman’s *Designing Data-Intensive Applications*, and, in particular, his thinking around unbundling databases and using change data capture in [Turning the database inside out with Apache Samza](https://martin.kleppmann.com/2015/03/04/turning-the-database-inside-out.html).

This package makes extensive use of prior art; particularly of use were [wal2json](https://github.com/eulerto/wal2json), [pgwire-replication](https://github.com/vnvo/pgwire-replication), [supabase/etl](https://github.com/supabase/etl).

There were no good turbopuffer Rust clients (the others I saw were largely untyped), so I cloned the Go/TS/Ruby/Java clients and had Claude build (and test!) a Rust one. I figured it was better to split out versus keep in this repo because it is more broadly useful. You can find it at [rs-puff](https://github.com/lucasgelfond/rs-puff) or with `cargo add rs-puff`
