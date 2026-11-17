use super::{active_scheduler, mount_source, split_device, stalls_under};
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

/// The active scheduler is the bracketed one, and a device with no
/// scheduler of its own lists `none` alone: neither is mistaken for BFQ.
#[test]
fn the_active_scheduler_is_the_bracketed_one() {
    assert_eq!(
        active_scheduler("none mq-deadline kyber [bfq]\n"),
        Some("bfq")
    );
    assert_eq!(
        active_scheduler("none [mq-deadline] kyber bfq\n"),
        Some("mq-deadline")
    );
    assert_eq!(active_scheduler("none\n"), None);
}

/// The mount a path lies on is the one whose mount point is its longest
/// prefix, and an escaped space in a mount point is read back as a space:
/// `/var/lib db` lies on the mount there, not on `/var`.
#[test]
fn the_longest_mount_point_over_a_path_gives_its_source() {
    let mountinfo = "\
22 1 8:2 / / rw,relatime - ext4 /dev/sda2 rw
40 22 0:45 / /var rw,noatime - btrfs /dev/mapper/vg-data rw,ssd
41 40 0:46 / /var/lib\\040db rw - xfs /dev/sdc1 rw
42 22 0:5 / /proc rw - proc proc rw
";
    assert_eq!(
        mount_source(mountinfo, Path::new("/var/cxbr/t/tree")),
        Some(PathBuf::from("/dev/mapper/vg-data"))
    );
    assert_eq!(
        mount_source(mountinfo, Path::new("/var/lib db/tree")),
        Some(PathBuf::from("/dev/sdc1"))
    );
    assert_eq!(
        mount_source(mountinfo, Path::new("/home/u")),
        Some(PathBuf::from("/dev/sda2"))
    );
    // A mount from no device node (proc, tmpfs) names no disk.
    assert_eq!(mount_source(mountinfo, Path::new("/proc/self")), None);
}

/// A device number splits into the major and minor numbers sysfs names its
/// device by, the high bits of each included.
#[test]
fn a_device_number_splits_as_the_kernel_encodes_it() {
    assert_eq!(split_device((8 << 8) | 0x11), (8, 17));
    assert_eq!(split_device(253 << 8), (253, 0));
    // minor 300 spills into the high minor bits.
    let wide = (259 << 8) | (0x12c & 0xff) | ((0x12c & !0xff) << 12);
    assert_eq!(split_device(wide), (259, 300));
}

/// A fake sysfs: a disk `sdb` with partition `sdb1` under the given
/// scheduler, and a device-mapper device `dm-0` with no scheduler of its own
/// stacked on `sdb1`; `dev/` holds the links the kernel names them by.
fn sysfs(scheduler: &str) -> std::io::Result<tempfile::TempDir> {
    let root = tempfile::tempdir()?;
    let disk = root.path().join("devices/sdb");
    std::fs::create_dir_all(disk.join("queue"))?;
    std::fs::write(disk.join("queue/scheduler"), scheduler)?;
    std::fs::create_dir_all(disk.join("sdb1"))?;
    std::fs::write(disk.join("sdb1/partition"), "1\n")?;
    let mapper = root.path().join("devices/dm-0");
    std::fs::create_dir_all(mapper.join("queue"))?;
    std::fs::write(mapper.join("queue/scheduler"), "none\n")?;
    std::fs::create_dir_all(mapper.join("slaves"))?;
    symlink(disk.join("sdb1"), mapper.join("slaves/sdb1"))?;
    std::fs::create_dir_all(root.path().join("dev"))?;
    symlink(disk.join("sdb1"), root.path().join("dev/8:17"))?;
    symlink(&mapper, root.path().join("dev/253:0"))?;
    Ok(root)
}

/// A partition answers with its disk's scheduler, and a device-mapper device
/// with the scheduler of the disk under it: the layout of a host whose
/// volume group sits on a BFQ disk.
#[test]
fn bfq_under_a_partition_or_a_mapped_device_is_found() -> std::io::Result<()> {
    let bfq = sysfs("none mq-deadline kyber [bfq]\n")?;
    assert!(stalls_under(&bfq.path().join("dev/8:17"), 0));
    assert!(stalls_under(&bfq.path().join("dev/253:0"), 0));

    let deadline = sysfs("none [mq-deadline] kyber bfq\n")?;
    assert!(!stalls_under(&deadline.path().join("dev/8:17"), 0));
    assert!(!stalls_under(&deadline.path().join("dev/253:0"), 0));
    Ok(())
}

/// A partition of a stacked device (`md0p1`) answers with the disks under
/// that device: the partition lists none of its own, its device does. Guards
/// against a tree on a partitioned RAID over BFQ disks answering no.
#[test]
fn bfq_under_a_partition_of_a_stacked_device_is_found() -> std::io::Result<()> {
    let root = sysfs("none mq-deadline kyber [bfq]\n")?;
    let array = root.path().join("devices/md0");
    std::fs::create_dir_all(array.join("queue"))?;
    std::fs::write(array.join("queue/scheduler"), "none\n")?;
    std::fs::create_dir_all(array.join("slaves"))?;
    symlink(
        root.path().join("devices/sdb/sdb1"),
        array.join("slaves/sdb1"),
    )?;
    std::fs::create_dir_all(array.join("md0p1"))?;
    std::fs::write(array.join("md0p1/partition"), "1\n")?;
    symlink(array.join("md0p1"), root.path().join("dev/259:1"))?;
    assert!(stalls_under(&root.path().join("dev/259:1"), 0));
    Ok(())
}

/// A device sysfs does not list answers no: the behaviour of a backend that
/// says nothing about its scheduler.
#[test]
fn an_unknown_device_answers_no() -> std::io::Result<()> {
    let root = sysfs("none mq-deadline kyber [bfq]\n")?;
    assert!(!stalls_under(&root.path().join("dev/1:2"), 0));
    Ok(())
}
