#[cfg(not(enable_cpuprofiler))]
pub fn start_profiler(_: &str) {}
#[cfg(not(enable_cpuprofiler))]
pub fn stop_profiler() {}

// #[cfg(feature = "enable_cpuprofiler")]
// pub fn start_profiler(name: &str) {
//     use cpuprofiler::PROFILER;
//     PROFILER.lock().unwrap().start(name).unwrap();
// }

// #[cfg(feature = "enable_cpuprofiler")]
// pub fn stop_profiler() {
//     use cpuprofiler::PROFILER;
//     PROFILER.lock().unwrap().stop().unwrap();
// }