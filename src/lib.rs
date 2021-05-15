#[macro_use]
extern crate error_chain;

pub mod errors {
    error_chain! {}
}
use errors::*;

use libc::stat;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    self, FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, Request, TimeOrNow,
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
    open_dirs: BTreeMap<Fh, (Dir, DirIter)>,
    open_files: BTreeMap<Fh, File>,
    inuse_fhs: BTreeSet<Fh>,
    inode_map: BTreeMap<Inode, PathBuf>,
}

impl PassFs {
    fn new(root_path: &str) -> Result<Self> {
        let root = Dir::open(root_path)
            .chain_err(|| format!("Unable to open passfs root directory {}", root_path))?;
        let mut passfs = PassFs {
            root,
            open_dirs: BTreeMap::new(),
            open_files: BTreeMap::new(),
            inuse_fhs: BTreeSet::new(),
            inode_map: BTreeMap::new(),
        };
        passfs.inode_map.insert(Inode(1), PathBuf::from("."));
        Ok(passfs)
    }

    fn get_fh(&mut self) -> Fh {
        let fd = Fh(self
            .inuse_fhs
            .iter()
            .enumerate()
            // Find the first gap in the list of open fds
            .find(|&(i, fd)| i as u64 != fd.value())
            .map(|(i, _)| i as u64)
            // If no gap, return the next number
            .unwrap_or(self.inuse_fhs.len() as u64));
        self.inuse_fhs.insert(fd);
        fd
    }
}

impl Filesystem for PassFs {
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
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

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
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

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        let path = match self.inode_map.get(&Inode(ino)) {
            Some(path) => path,
            None => return reply.error(libc::ENOENT),
        };

        let dir = match self.root.sub_dir(path) {
            Ok(dir) => dir,
            Err(err) => return reply.error(err.raw_os_error().unwrap_or(libc::EIO)),
        };

        match dir.list_dir(".") {
            Ok(iter) => {
                let fh = self.get_fh();
                self.open_dirs.insert(fh, (dir, iter));
                reply.opened(fh.value(), 0)
            }
            Err(err) => {
                let err = err.raw_os_error().unwrap_or(libc::EIO);
                if err == libc::ENOENT {
                    self.inode_map.remove(&Inode(ino));
                }
                reply.error(err)
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if offset < 0 {
            return reply.error(libc::EINVAL);
        }

        let (dir, diriter) = match self.open_dirs.get_mut(&Fh(fh)) {
            None => {
                reply.error(libc::EBADFD);
                return;
            }
            Some(dir) => dir,
        };

        for entry in diriter {
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

                    // Unfortunately, although the dirent retrived by the openat
                    // library contains the inode they decided not to give it to
                    // us. We make another system call to fetch it for realz
                    // this time.
                    let file_name = entry.file_name();

                    let metadata = match dir.metadata(file_name) {
                        Ok(metadata) => metadata,
                        Err(err) => return reply.error(err.raw_os_error().unwrap_or(libc::EIO)),
                    };

                    if reply.add(metadata.stat().st_ino, 0, kind, file_name) {
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

    fn releasedir(&mut self, _req: &Request, _ino: u64, fh: u64, _flags: i32, reply: ReplyEmpty) {
        let fh = Fh(fh);
        if self.open_dirs.remove(&fh).is_none() {
            warn!("releasedir, but {:?} is not in open_dirs", fh)
        }

        if !self.inuse_fhs.remove(&fh) {
            warn!("releasedir, but {:?} is not in inuse_fhs", fh)
        }

        reply.ok()
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let mask = libc::O_APPEND | libc::O_CREAT | libc::O_TRUNC;
        debug! {"Flags: {:o}, Mask: {:o}", flags, mask};

        if flags & mask != 0 {
            return reply.error(libc::EROFS);
        }

        let path = match self.inode_map.get(&Inode(ino)) {
            Some(path) => path,
            None => return reply.error(libc::ENOENT),
        };

        match self.root.open_file(path) {
            Ok(file) => {
                let fh = self.get_fh();
                self.open_files.insert(fh, file);
                reply.opened(fh.value(), 0)
            }
            Err(err) => {
                let err = err.raw_os_error().unwrap_or(libc::EIO);
                if err == libc::ENOENT {
                    self.inode_map.remove(&Inode(ino));
                }
                reply.error(err)
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            return reply.error(libc::EINVAL);
        }

        let fh = Fh(fh);
        let mut file = match self.open_files.get(&fh) {
            Some(file) => file,
            None => return reply.error(libc::EBADFD),
        };
        if let Err(err) = file.seek(SeekFrom::Start(offset as u64)) {
            return reply.error(err.raw_os_error().unwrap_or(libc::EIO));
        }

        debug!("File: {:?}", file);

        let mut buffer = vec![0u8; size as usize];
        let mut pos = 0;
        while pos < buffer.len() {
            debug!("Buflen: {}", buffer.len());
            match file.read(&mut buffer[pos..]) {
                Ok(0) => break,
                Ok(bytesin) => pos += bytesin,
                Err(err) => return reply.error(err.raw_os_error().unwrap_or(libc::EIO)),
            }
        }
        reply.data(&buffer[..pos])
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let fh = Fh(fh);
        if self.open_files.remove(&fh).is_none() {
            warn!("release, but {:?} is not in open_files", fh)
        }

        if !self.inuse_fhs.remove(&fh) {
            warn!("release, but {:?} is not in inuse_fhs", fh)
        }

        reply.ok()
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        reply.error(libc::EROFS)
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        reply.error(libc::EROFS)
    }

    fn setxattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(libc::EROFS)
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        reply.error(libc::EPERM)
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
