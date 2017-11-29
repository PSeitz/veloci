# native_search [![Build Status](https://travis-ci.org/PSeitz/native_search.svg?branch=master)](https://travis-ci.org/PSeitz/native_search)

CARGO_INCREMENTAL=1 RUST_BACKTRACE=full RUST_TEST_THREADS=1 RUST_LOG=search_lib=trace,measure_time=info nodemon -e rs --exec "cargo test 'test::' -- --nocapture || exit 1"
