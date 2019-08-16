# Veloci [![Build Status](https://travis-ci.org/PSeitz/veloci.svg?branch=master)](https://travis-ci.org/PSeitz/veloci) [![Coverage Status](https://coveralls.io/repos/github/PSeitz/veloci/badge.svg?branch=master)](https://coveralls.io/github/PSeitz/veloci?branch=master) [![codecov](https://codecov.io/gh/PSeitz/veloci/branch/master/graph/badge.svg)](https://codecov.io/gh/PSeitz/veloci) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

LoadingType=Disk CARGO_INCREMENTAL=1 RUST_BACKTRACE=full RUST_TEST_THREADS=1 RUST_LOG=search_lib=trace,measure_time=info cargo watch -w src -x 'test -- --nocapture'


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
- Support for In-Memory and MMap Indices
- Speed


## Webserver

To install the search enginge bundled with the webserver execute in the `server` folder:
`cd server;cargo install`

To start the server and load search indices inside the jmdict folder:
`LoadingType=InMemory ROCKET_ENV=stage RUST_BACKTRACE=1 RUST_LOG=search_lib=info ROCKET_PORT=3000 rocket_server jmdict`


