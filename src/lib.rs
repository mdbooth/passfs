#[macro_use]
extern crate error_chain;

pub mod errors {
    error_chain! {}
}
use errors::*;

use libc::stat;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    self, FileAttr, FileType, Filesystem, ReplyAttr, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyOpen, Request,
};
use log::{debug, warn};
use openat::{self, Dir, DirIter, SimpleType};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct Fh(u64);

impl Fh {
    fn value(self) -> u64 {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
struct Inode(u64);

pub struct PassFs {
    root: Dir,
    open_dirs: BTreeMap<Fh, DirIter>,
    open_fhs: BTreeSet<Fh>,
    inode_map: BTreeMap<Inode, PathBuf>,
}

impl PassFs {
    fn new(root_path: &str) -> Result<Self> {
        let root = Dir::open(root_path)
            .chain_err(|| format!("Unable to open passfs root directory {}", root_path))?;
        let mut passfs = PassFs {
            root,
            open_dirs: BTreeMap::new(),
            open_fhs: BTreeSet::new(),
            inode_map: BTreeMap::new(),
        };
        passfs.inode_map.insert(Inode(1), PathBuf::from("."));
        Ok(passfs)
    }

    fn get_fh(&mut self) -> Fh {
        let fd = Fh(self
            .open_fhs
            .iter()
            .enumerate()
            // Find the first gap in the list of open fds
            .find(|&(i, fd)| i as u64 != fd.value())
            .map(|(i, _)| i as u64)
            // If no gap, return the next number
            .unwrap_or(self.open_fhs.len() as u64));
        self.open_fhs.insert(fd);
        fd
    }
}

impl Filesystem for PassFs {
    fn getattr(&mut self, req: &Request, ino: u64, reply: ReplyAttr) {
        debug!("getattr(req={:?}, ino={:?})", req, ino);

        let path = match self.inode_map.get(&Inode(ino)) {
            Some(path) => path,
            None => return reply.error(libc::ENOENT),
        };

        let metadata = self.root.metadata(path);
        match metadata {
            Ok(metadata) => reply.attr(&Duration::new(0, 0), &stat_to_fileattr(&metadata.stat())),
            Err(err) => {
                let err = err.raw_os_error().unwrap_or(libc::EIO);
                reply.error(err);
                self.inode_map.remove(&Inode(ino));
            }
        }
    }

    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        debug!(
            "lookup(req={:?}, parent={:?}, name={:?})",
            req, parent, name
        );

        let mut path = if parent == 1 {
            PathBuf::new()
        } else {
            match self.inode_map.get(&Inode(parent)) {
                Some(path) => path.clone(),
                None => return reply.error(libc::ENOENT),
            }
        };
        path.push(name);

        let metadata = self.root.metadata(&path);
        match metadata {
            Ok(metadata) => {
                let fileattr = stat_to_fileattr(&metadata.stat());
                reply.entry(&Duration::new(0, 0), &fileattr, 0);
                self.inode_map.insert(Inode(fileattr.ino), path);
            }
            Err(err) => reply.error(err.raw_os_error().unwrap_or(libc::EIO)),
        }
    }

    fn opendir(&mut self, req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
        debug!("opendir(req={:?}, ino={:?}, flags={:?})", req, ino, flags);

        let path = match self.inode_map.get(&Inode(ino)) {
            Some(path) => path,
            None => return reply.error(libc::ENOENT),
        };

        match self.root.list_dir(path) {
            Ok(iter) => {
                let fd = self.get_fh();
                self.open_dirs.insert(fd, iter);
                reply.opened(fd.value(), 0)
            }
            Err(err) => reply.error(err.raw_os_error().unwrap_or(libc::EIO)),
        }
    }

    fn readdir(
        &mut self,
        req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        debug!(
            "readdir(req={:?}, ino={:?}, fh={:?}, offset={:?})",
            req, ino, fh, offset
        );

        let dir = self.open_dirs.get_mut(&Fh(fh));
        let dir = match dir {
            None => {
                reply.error(libc::EINVAL);
                return;
            }
            Some(dir) => dir,
        };

        for entry in dir {
            match entry {
                Ok(entry) => {
                    debug!("Entry: {:?}", entry.file_name());
                    let kind = match entry.simple_type() {
                        Some(SimpleType::Symlink) => FileType::Symlink,
                        Some(SimpleType::Dir) => FileType::Directory,
                        Some(SimpleType::File) => FileType::RegularFile,
                        // CharDevice is our catch-all weird device type here.
                        // It looks like you really can't extract the actual
                        // data from Entry
                        Some(SimpleType::Other) => FileType::CharDevice,
                        // WTF does None mean here?
                        None => FileType::CharDevice,
                    };
                    if reply.add(ino, 0, kind, entry.file_name()) {
                        // add returns true if the reply buffer is full
                        return reply.ok();
                    }
                }
                Err(err) => {
                    debug!("Error: {}", err);
                    return reply.error(err.raw_os_error().unwrap_or(libc::EIO));
                }
            }
        }
        reply.ok()
    }

    fn releasedir(&mut self, req: &Request, _ino: u64, fh: u64, flags: i32, reply: ReplyEmpty) {
        debug!(
            "releasedir(req={:?}, ino={:?}, fh={:?}, flags={:?})",
            req, _ino, fh, flags
        );

        let fh = Fh(fh);
        if self.open_dirs.remove(&fh).is_none() {
            warn!("releasedir, but {:?} is not in open_dirs", fh)
        }

        if !self.open_fhs.remove(&fh) {
            warn!("releasedir, but {:?} is not in open_fhs", fh)
        }

        reply.ok()
    }
}

fn stat_to_fileattr(stat: &stat) -> FileAttr {
    let kind = match stat.st_mode & libc::S_IFMT {
        libc::S_IFSOCK => FileType::Socket,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFREG => FileType::RegularFile,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFCHR => FileType::CharDevice,
        libc::S_IFIFO => FileType::NamedPipe,
        _ => {
            warn! {"Unrecognised file type {:o} for inode {:x}", stat.st_mode, stat.st_ino};
            FileType::RegularFile
        }
    };

    fn get_system_time(time: i64) -> SystemTime {
        UNIX_EPOCH
            .checked_add(Duration::new(time as u64, 0))
            .unwrap_or(UNIX_EPOCH)
    }

    FileAttr {
        ino: stat.st_ino,
        size: stat.st_ino,
        blocks: stat.st_blocks as u64,
        atime: get_system_time(stat.st_atime),
        mtime: get_system_time(stat.st_mtime),
        ctime: get_system_time(stat.st_ctime),
        crtime: UNIX_EPOCH,
        kind,
        perm: (stat.st_mode & 0o777) as u16,
        nlink: stat.st_nlink as u32,
        uid: stat.st_uid,
        gid: stat.st_gid,
        rdev: stat.st_rdev as u32,
        blksize: stat.st_blksize as u32,
        padding: 0,
        flags: 0,
    }
}

pub fn run(mountpoint: &str, root_path: &str) -> Result<()> {
    let path = Path::new(mountpoint);
    let mountopts: &[&OsStr] = &[];
    let passfs = PassFs::new(root_path)?;

    fuser::mount(passfs, &path, mountopts)
        .chain_err(|| format!("Error mounting passfs on {}", mountpoint))
}
