[profile.release] debug = true

valgrind --tool=callgrind --dump-instr=yes --collect-jumps=yes --simulate-cache=yes ./target/release/fstest-rs

$./gprof2dot.py --format=callgrind --output=out.dot /path/to/callgrind.out $ dot -Tpng out.dot -o graph.png 