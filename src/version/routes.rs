// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The level routes a tree's manifest records, and what an open after an
//! unclean shutdown checks against them.
//!
//! A synced flush returns with no manifest edit naming its tables: after a
//! crash only the tables describe it, in whichever tables folder its level was
//! routed to. An open whose configuration leaves that folder out, or finds it
//! missing (a volume not mounted), would not see them and would drop data the
//! flush acknowledged. So every snapshot records the routes of the open that
//! wrote it, each as its tables folder relative to the tree's folder (a tree
//! moved with its routes keeps them), and a clean close appends an edit saying
//! the manifest names every flush. An open that finds no such edit last
//! refuses while a recorded route is not configured or its folder is gone.

use crate::Config;
use crate::path::Path;
use alloc::borrow::Cow;
use alloc::string::String;
use alloc::vec::Vec;

/// The text of `path`, its separators as they are, each `%` and each byte
/// that is not UTF-8 written as `%XX`: two different folders never read
/// alike, as they would with the bytes replaced.
#[cfg(feature = "std")]
fn text(path: &Path) -> Cow<'_, str> {
    if let Some(text) = path.to_str() {
        return escape_percent(text);
    }
    let mut out = String::new();
    for chunk in path.as_os_str().as_encoded_bytes().utf8_chunks() {
        out.push_str(&escape_percent(chunk.valid()));
        for byte in chunk.invalid() {
            push_escaped(&mut out, *byte);
        }
    }
    Cow::Owned(out)
}

/// The text of `path`, its separators as they are, each `%` written as `%25`
/// as with std, so both builds record a folder alike.
#[cfg(not(feature = "std"))]
fn text(path: &Path) -> Cow<'_, str> {
    escape_percent(path.as_ref())
}

/// `text` with each `%` written as `%25`.
fn escape_percent(text: &str) -> Cow<'_, str> {
    if !text.contains('%') {
        return Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len() + 2);
    for c in text.chars() {
        if c == '%' {
            push_escaped(&mut out, b'%');
        } else {
            out.push(c);
        }
    }
    Cow::Owned(out)
}

/// Appends `byte` as `%XX`.
fn push_escaped(out: &mut String, byte: u8) {
    out.push('%');
    for nibble in [byte >> 4, byte & 0xF] {
        // A nibble is below 16, so it is a hex digit.
        let digit = char::from_digit(u32::from(nibble), 16).unwrap_or('0');
        out.push(digit.to_ascii_uppercase());
    }
}

/// The separators of a path on this platform: off Windows a backslash is a
/// character of a name.
#[cfg(windows)]
const SEPARATORS: &[char] = &['/', '\\'];
/// The separators of a path on this platform: off Windows a backslash is a
/// character of a name.
#[cfg(not(windows))]
const SEPARATORS: &[char] = &['/'];

/// The named parts of `text`, split at this platform's separators.
fn parts(text: &str) -> Vec<&str> {
    text.split(SEPARATORS)
        .filter(|part| !part.is_empty() && *part != ".")
        .collect()
}

/// `folder` relative to `base`, its parts joined by `/`: a `..` for each part
/// of `base` past the parts they share.
pub(crate) fn relative(base: &Path, folder: &Path) -> String {
    let (base, folder) = (text(base), text(folder));
    let (base, folder) = (parts(&base), parts(&folder));
    let shared = base.iter().zip(&folder).take_while(|(a, b)| a == b).count();
    // `shared` counts a prefix of `base`, so it is at most its length.
    let mut out: Vec<&str> = core::iter::repeat_n("..", base.len() - shared).collect();
    out.extend(folder.iter().skip(shared));
    out.join("/")
}

/// The tables folders of `config`'s level routes, relative to the tree's
/// folder: what a snapshot records.
pub(crate) fn of(config: &Config) -> Vec<String> {
    config
        .all_tables_folders()
        .into_iter()
        // The first is the tree's own.
        .skip(1)
        .map(|(folder, _)| relative(&config.path, &folder))
        .collect()
}

/// Refuses an open after an unclean shutdown when a route the manifest
/// records is not configured now, or its tables folder is missing.
///
/// # Errors
///
/// [`crate::Error::MissingRoute`] for the first such route, or an I/O error
/// asking whether its folder exists.
pub(crate) fn check(
    recorded: &[String],
    closed_cleanly: bool,
    config: &Config,
) -> crate::Result<()> {
    if closed_cleanly || recorded.is_empty() {
        return Ok(());
    }
    let folders = config.all_tables_folders();
    for route in recorded {
        let configured = folders
            .iter()
            .skip(1)
            .find(|(folder, _)| relative(&config.path, folder) == *route);
        let reachable = match configured {
            Some((folder, fs)) => fs.exists(folder)?,
            None => false,
        };
        if !reachable {
            log::error!(
                "the tree was not closed cleanly and its level route {route} is not configured \
                 or its folder is missing: flushes after the last manifest may lie there"
            );
            return Err(crate::Error::MissingRoute {
                folder: route.clone(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
