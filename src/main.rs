use error_chain::ChainedError;

use simple_logger::SimpleLogger;
use std::process;

const MOUNTPOINT: &str = "/tmp/passfs";
const ROOT_PATH: &str = "/home/mbooth/tmp";

fn main() {
    SimpleLogger::new().init().unwrap();

    if let Err(err) = passfs::run(MOUNTPOINT, ROOT_PATH) {
        eprintln!("{}", err.display_chain().to_string());
        process::exit(1)
    }
}
