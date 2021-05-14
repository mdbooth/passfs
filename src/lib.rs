#[macro_use]
extern crate error_chain;

pub mod errors {
    error_chain!{}
}
use errors::*;

use fuse::{self, Filesystem};
use std::path::Path;
use std::ffi::OsStr;

pub struct PassFS;
impl Filesystem for PassFS {}

pub fn run(mountpoint: &str) -> Result<()> {
    let path = Path::new(mountpoint);
    let mountopts: &[&OsStr] = &[];

    fuse::mount(PassFS{}, &path, mountopts).chain_err(|| format!("Error mounting passfs on {}", mountpoint))
}