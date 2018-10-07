# Veloci [![Build Status](https://travis-ci.org/PSeitz/native_search.svg?branch=master)](https://travis-ci.org/PSeitz/native_search) [![Coverage Status](https://coveralls.io/repos/github/PSeitz/native_search/badge.svg?branch=master)](https://coveralls.io/github/PSeitz/native_search?branch=master) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

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
