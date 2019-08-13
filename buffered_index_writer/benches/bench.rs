#[macro_use]
extern crate criterion;

use buffered_index_writer::*;

use criterion::{Criterion, *};

fn pseudo_rand(i: u32) -> u32 {
    if i % 2 == 0 {
        ((i as u64 * i as u64) % 16_000) as u32
    } else {
        i % 3
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let plot_config = PlotConfiguration::default().summary_scale(AxisScale::Logarithmic);

    let parameters = vec![250, 2_500, 100_000, 150_000, 190_000, 250_000, 2_500_000];
    let benchmark = ParameterizedBenchmark::new(
        "buffered",
        |b, i| {
            b.iter(|| {
                let mut ind = BufferedIndexWriter::new_unstable_sorted();
                for i in 0..*i {
                    ind.add(i, pseudo_rand(i)).unwrap();
                }
            })
        },
        parameters,
    )
    .plot_config(plot_config)
    .throughput(|s| Throughput::Bytes(s * 8 as u32));
    c.bench("insert throughput", benchmark);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
