use log::Record;
use flexi_logger;
use chrono::Local;
use parking_lot::RwLock;

/// A logline-formatter that produces log lines like
/// <br>
/// ```[2016-01-13 15:25:01.640870 +01:00] INFO [src/foo/bar:26] Task successfully read from conf.json```
/// <br>
/// i.e. with timestamp and file location.
pub fn format_log(record: &Record) -> String {
    if module_path!().split("::").nth(0) == record.module_path().unwrap_or("<unnamed>").split("::").nth(0) {
        format!(
            "[{}] {} [{}:{}] {}",
            Local::now().format("%Y-%m-%d %H:%M:%S%.6f %:z"),
            record.level(),
            record.file().unwrap_or("<unnamed>"),
            record.line().unwrap_or(0),
            &record.args()
        )
    } else {
        format!(
            "[{}] {} [{}] {}",
            Local::now().format("%Y-%m-%d %H:%M:%S%.6f %:z"),
            record.level(),
            record.module_path().unwrap_or("<unnamed>"),
            &record.args()
        )
    }
}

static LOG_ENABLED: RwLock<bool> = RwLock::new(false);

pub fn enable_log() {
    let mut log_enabledo = LOG_ENABLED.write();
    {
        if !*log_enabledo {
            flexi_logger::Logger::with_env().format(format_log).start().unwrap();
            *log_enabledo = true;
        }
    };
}
