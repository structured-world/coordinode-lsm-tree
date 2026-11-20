use super::{
    active_scheduler, block_device, fdinfo_mount_id, idles_between_queues, listed_device, mount_id,
    mount_source, split_device, stalls_under,
};
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

/// A folder on a file system with a device number of its own (`/proc`) is
/// looked up through its mount, which a descriptor of it names, and that
/// mount's source is no device node: no disk, and no BFQ. Guards against
/// such a folder being taken for a disk, or the lookup failing on the way.
#[test]
fn a_folder_on_a_mount_from_no_device_names_no_disk() {
    assert!(mount_id(Path::new("/proc")).is_some(), "a descriptor of it");
    assert_eq!(block_device(Path::new("/proc")), None);
    assert!(!idles_between_queues(Path::new("/proc")));
}

/// A folder that cannot be opened has no mount to name.
#[test]
fn a_missing_folder_has_no_mount() {
    assert_eq!(mount_id(Path::new("/proc/no such folder")), None);
}

/// A relative folder not made yet lies on the working folder's device, as the
/// same folder spelled from `./` does. Guards against a tree created at a
/// relative path asking an empty path, which names no device, and opening
/// without the strategy its disk calls for.
#[test]
fn a_missing_relative_folder_lies_on_the_working_folders_device() {
    let here = block_device(Path::new("."));
    assert_eq!(block_device(Path::new("no such folder here")), here);
    assert_eq!(block_device(Path::new("./no such folder here")), here);
}

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

/// A mount's source is found by its id, the first field of its line, and an
/// escaped space in a source is read back as a space.
#[test]
fn a_mount_id_gives_its_source() {
    let mountinfo = "\
22 1 8:2 / / rw,relatime - ext4 /dev/sda2 rw
40 22 0:45 / /var rw,noatime - btrfs /dev/mapper/vg-data rw,ssd
41 40 0:46 / /var/lib\\040db rw - xfs /dev/my\\040disk rw
42 22 0:5 / /proc rw - proc proc rw
";
    let mountinfo = mountinfo.as_bytes();
    assert_eq!(
        mount_source(mountinfo, 40),
        Some(PathBuf::from("/dev/mapper/vg-data"))
    );
    assert_eq!(
        mount_source(mountinfo, 41),
        Some(PathBuf::from("/dev/my disk"))
    );
    assert_eq!(
        mount_source(mountinfo, 22),
        Some(PathBuf::from("/dev/sda2"))
    );
    // A mount from no device node (proc, tmpfs) names no disk, and an id no
    // line has names nothing.
    assert_eq!(mount_source(mountinfo, 42), None);
    assert_eq!(mount_source(mountinfo, 4), None);
}

/// A byte that is not UTF-8, in another mount's line or in the source itself,
/// does not hide the source: proc(5) writes a path's bytes as they are.
/// Guards against one such mount point anywhere on the host turning the
/// probe off for every tree.
#[test]
fn a_byte_that_is_not_utf8_does_not_hide_the_source() {
    use std::os::unix::ffi::OsStrExt as _;
    let mut mountinfo = b"30 22 0:50 / /media/\xff rw - vfat /dev/sdb1 rw\n".to_vec();
    mountinfo.extend_from_slice(b"31 22 8:2 / /data rw - ext4 /dev/disk\xfe rw\n");
    mountinfo.extend_from_slice(b"32 22 8:3 / /srv rw - xfs /dev/sdc1 rw\n");
    assert_eq!(
        mount_source(&mountinfo, 32),
        Some(PathBuf::from("/dev/sdc1"))
    );
    let Some(odd) = mount_source(&mountinfo, 31) else {
        panic!("a source that is not UTF-8 is found");
    };
    assert_eq!(odd.as_os_str().as_bytes(), b"/dev/disk\xfe");
}

/// Lines of `/proc/self/mountinfo` that are cut short are skipped, and a
/// backslash that starts no octal escape is kept as it is.
#[test]
fn a_malformed_mountinfo_line_is_skipped() {
    let mountinfo = "\
no separator in this line
23 1
24 22 8:3 / /data rw - ext4
x 22 8:3 / /data rw - ext4 /dev/sdz1 rw
26 22 8:5 / /odd rw - xfs /dev/sde\\x1 rw
";
    let mountinfo = mountinfo.as_bytes();
    assert_eq!(mount_source(mountinfo, 24), None, "a line without a source");
    assert_eq!(
        mount_source(mountinfo, 26),
        Some(PathBuf::from("/dev/sde\\x1")),
        "a backslash that starts no escape stays"
    );
}

/// The mount id of a descriptor is the `mnt_id` line of its `fdinfo`.
#[test]
fn a_descriptors_mount_id_is_read_from_its_fdinfo() {
    let info = "pos:\t0\nflags:\t012100000\nmnt_id:\t97\nino:\t256\n";
    assert_eq!(fdinfo_mount_id(info), Some(97));
    assert_eq!(fdinfo_mount_id("pos:\t0\n"), None);
}

/// A stack of devices deeper than any real one answers no rather than
/// following links forever: a loop in a fake sysfs ends.
#[test]
fn a_device_stack_past_the_limit_answers_no() -> std::io::Result<()> {
    let root = tempfile::tempdir()?;
    let a = root.path().join("devices/dm-0");
    let b = root.path().join("devices/dm-1");
    for device in [&a, &b] {
        std::fs::create_dir_all(device.join("queue"))?;
        std::fs::write(device.join("queue/scheduler"), "none\n")?;
        std::fs::create_dir_all(device.join("slaves"))?;
    }
    symlink(&b, a.join("slaves/dm-1"))?;
    symlink(&a, b.join("slaves/dm-0"))?;
    assert!(!stalls_under(&a, 0));
    Ok(())
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

/// A file's own device number is taken as its disk when sysfs lists it,
/// without reading the mounts; a file system's own number (major 0, as
/// Btrfs and overlay give) or one sysfs does not list is not, and the mounts
/// decide. Guards against a Btrfs file's anonymous device being read as a
/// disk.
#[test]
fn a_files_own_device_is_its_disk_only_when_sysfs_lists_it() -> std::io::Result<()> {
    let root = sysfs("none mq-deadline kyber [bfq]\n")?;
    let block = root.path().join("dev");
    let partition = (8 << 8) | 0x11;
    assert_eq!(listed_device(partition, &block), Some(partition));
    assert_eq!(listed_device(45, &block), None, "major 0");
    assert_eq!(listed_device((1 << 8) | 2, &block), None, "not listed");
    Ok(())
}

/// BFQ with `slice_idle` set to zero does not idle between queues, unless
/// `strict_guarantees` forces it to; a stacked device over such a disk
/// answers the same. Guards against a host that turned BFQ's idling off
/// still getting the batching and serialization meant for a scheduler that
/// idles.
#[test]
fn bfq_with_idling_turned_off_does_not_idle() -> std::io::Result<()> {
    let root = sysfs("none mq-deadline kyber [bfq]\n")?;
    let iosched = root.path().join("devices/sdb/queue/iosched");
    std::fs::create_dir_all(&iosched)?;
    std::fs::write(iosched.join("slice_idle"), "0\n")?;
    std::fs::write(iosched.join("strict_guarantees"), "0\n")?;
    assert!(!stalls_under(&root.path().join("dev/8:17"), 0));
    assert!(!stalls_under(&root.path().join("dev/253:0"), 0));

    std::fs::write(iosched.join("strict_guarantees"), "1\n")?;
    assert!(stalls_under(&root.path().join("dev/8:17"), 0), "forced on");

    std::fs::write(iosched.join("strict_guarantees"), "0\n")?;
    std::fs::write(iosched.join("slice_idle"), "8\n")?;
    assert!(
        stalls_under(&root.path().join("dev/253:0"), 0),
        "the default"
    );

    // `slice_idle` shows whole milliseconds: an interval below one reads as
    // zero there, and only `slice_idle_us` tells it from idling turned off.
    std::fs::write(iosched.join("slice_idle"), "0\n")?;
    std::fs::write(iosched.join("slice_idle_us"), "500\n")?;
    assert!(
        stalls_under(&root.path().join("dev/8:17"), 0),
        "an interval under a millisecond"
    );
    std::fs::write(iosched.join("slice_idle_us"), "0\n")?;
    assert!(
        !stalls_under(&root.path().join("dev/8:17"), 0),
        "zero microseconds"
    );
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
