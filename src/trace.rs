use chrono::Local;
use flexi_logger;
use log::Record;
use parking_lot::RwLock;
use std::io;
use std::io::Write;

/// A logline-formatter that produces log lines like
/// <br>
/// ```[2016-01-13 15:25:01.640870 +01:00] INFO [src/foo/bar:26] Task successfully read from conf.json```
/// <br>
/// i.e. with timestamp and file location.
#[cfg_attr(tarpaulin, skip)]
pub fn format_log(w: &mut Write, record: &Record<'_>) -> Result<(), io::Error> {
    write!(
        w,
        "[{}] {} [{}:{}] {}",
        Local::now().format("%Y-%m-%d %H:%M:%S%.6f %:z"),
        record.level(),
        record.file().unwrap_or("<unnamed>"),
        record.line().unwrap_or(0),
        &record.args()
    )
}

static LOG_ENABLED: RwLock<bool> = RwLock::new(false);
#[cfg_attr(tarpaulin, skip)]
pub fn enable_log() {
    let mut log_enabledo = LOG_ENABLED.write();
    if !*log_enabledo {
        flexi_logger::Logger::with_env().format(format_log).start().unwrap();
        *log_enabledo = true;
    };
}
