use log::LogRecord;
use flexi_logger;
use chrono::Local;

/// A logline-formatter that produces log lines like
/// <br>
/// ```[2016-01-13 15:25:01.640870 +01:00] INFO [src/foo/bar:26] Task successfully read from conf.json```
/// <br>
/// i.e. with timestamp and file location.
pub fn format_log(record: &LogRecord) -> String {
    if module_path!().split("::").nth(0) == record.location().module_path().split("::").nth(0) {
        format!("[{}] {} [{}:{}] {}",
            Local::now().format("%Y-%m-%d %H:%M:%S%.6f %:z"),
            record.level(),
            record.location().file(),
            record.location().line(),
            &record.args())
    }else{
        format!("[{}] {} [{}] {}",
            Local::now().format("%Y-%m-%d %H:%M:%S%.6f %:z"),
            record.level(),
            record.location().module_path(),
            &record.args())
    }
}

pub fn enable_log(){
    flexi_logger::Logger::with_env().format(format_log).start().unwrap();
}