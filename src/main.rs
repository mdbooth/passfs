use passfs;
use error_chain::ChainedError;

use std::process;

const MOUNTPOINT: &str = "/tmp/passfs";

fn main() {
    if let Err(err) = passfs::run(MOUNTPOINT) {
        eprintln!("{}", err.display_chain().to_string());
        process::exit(1)
    }
}