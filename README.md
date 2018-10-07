# Veloci [![Build Status](https://travis-ci.org/PSeitz/veloci.svg?branch=master)](https://travis-ci.org/PSeitz/veloci) [![Coverage Status](https://coveralls.io/repos/github/PSeitz/veloci/badge.svg?branch=master)](https://coveralls.io/github/PSeitz/veloci?branch=master) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

LoadingType=Disk CARGO_INCREMENTAL=1 RUST_BACKTRACE=full RUST_TEST_THREADS=1 RUST_LOG=search_lib=trace,measure_time=info cargo watch -w src -x 'test tests::  -- --nocapture'


## Features

- Fuzzy Search
- Query Boosting
- Term Boosting
- Phrase Boosting
- Facets
- Filters
- Stopwordlists (EN, DE)
- Queryparser
- Compressed Docstore
- Support for In-Memory and MMap for Indices
- Speed
