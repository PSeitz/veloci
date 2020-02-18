use chrono::Local;
// use flexi_logger;
// use log::Record;
use parking_lot::RwLock;
use std::io::Write;

// /// A logline-formatter that produces log lines like
// /// <br>
// /// ```[2016-01-13 15:25:01.640870 +01:00] INFO [src/foo/bar:26] Task successfully read from conf.json```
// /// <br>
// /// i.e. with timestamp and file location.
// #[cfg_attr(tarpaulin, skip)]
// pub fn format_log(w: &mut dyn Write, record: &Record<'_>) -> Result<(), io::Error> {
//     write!(
//         w,
//         "[{}] {} [{}:{}] {}",
//         Local::now().format("%Y-%m-%d %H:%M:%S%.6f %:z"),
//         record.level(),
//         record.file().unwrap_or("<unnamed>"),
//         record.line().unwrap_or(0),
//         &record.args()
//     )
// }

// fn to_level_filter(text: &str) -> log::LevelFilter {
//     match &text.to_lowercase() {
//         "error" => log::LevelFilter::Error,
//         "warn" => log::LevelFilter::Warn,
//         "info" => log::LevelFilter::Info,
//         "debug" => log::LevelFilter::Debug,
//         "trace" => log::LevelFilter::Trace,
//         None => log::LevelFilter::Off,
//     }
// }

// #[test]
// fn test_parse_env_string(text: &str) {
// }
// #[test]
// fn parse_env_string(text: &str) {
// match env::var("RUST_LOG") {
//     Ok(logo) => {

//     },
//     Err(..) => Ok(Self::off()),
// }
//     // veloci=info,otherlib=warn
//     text.split(",").map(|el|)
// }

static LOG_ENABLED: RwLock<bool> = RwLock::new(false);
#[cfg_attr(tarpaulin, skip)]
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

        // flexi_logger::Logger::with_env().format(format_log).start().expect("could not create logger");
        *log_enabledo = true;
    };
}
