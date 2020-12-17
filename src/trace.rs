use chrono::Local;
// use flexi_logger;
// use log::Record;
use parking_lot::RwLock;
use std::io::Write;

static LOG_ENABLED: RwLock<bool> = RwLock::new(false);
#[cfg(not(tarpaulin_include))]
pub fn enable_log() {
    let mut log_enabledo = LOG_ENABLED.write();

    if !*log_enabledo {
        env_logger::builder()
            .format(|w, record| {
                writeln!(
                    w,
                    "[{}] {} [{}:{}] {}",
                    Local::now().format("%Y-%m-%d %H:%M:%S%.6f %:z"),
                    record.level(),
                    record.file().unwrap_or("<unnamed>"),
                    record.line().unwrap_or(0),
                    &record.args()
                )
            })
            .init();

        *log_enabledo = true;
    };
}
