//! geocoder-transport encode <kind> <dir> [<file>]
//!   writes the transport unit of the member files in <dir> to stdout: the stream before zstd
//!   (query_server::transport). <kind> is places, pois, arcs or cols; cols takes its file, one of
//!   transport::COLS_FILES.
//! geocoder-transport decode <unit.tt.zst | -> <dir>
//!   decodes a unit (zstd-compressed; - reads it from stdin) into its members in <dir>, each
//!   written in a hidden dir of the decode's own there and all moved into place once the whole
//!   unit has decoded, once it has finished what decodes killed there left; on an error <dir>
//!   is as it was once those were finished (transport::decode_into_dir).
//! geocoder-transport verify <unit.tt.zst | -> <dir>
//!   decodes a unit and compares each member with the file of its name in <dir>, writing nothing
//!   there (a places unit's block table goes to a scratch file in the temp dir while it runs).
//! Each prints the unit's kind and members on success. Exit status: 0 done; 1 a unit or file
//! refused, a member that differs, a file it can't read or write; 2 usage.
use query_server::transport::{self, Kind, Member};
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::path::Path;
use std::process::ExitCode;

const USAGE: &str = "usage: geocoder-transport encode <kind> <dir> [<file>] | decode <unit.tt.zst|-> <dir> | verify <unit.tt.zst|-> <dir>";

/// The unit at `path` (- for stdin), as published.
fn published(path: &str) -> io::Result<Box<dyn Read>> {
    Ok(if path == "-" {
        Box::new(io::stdin().lock())
    } else {
        Box::new(File::open(path).map_err(|e| io::Error::new(e.kind(), format!("{path}: {e}")))?)
    })
}

/// The unit at `path` (- for stdin), through zstd.
fn unit_reader(path: &str) -> io::Result<zstd::stream::read::Decoder<'static, BufReader<Box<dyn Read>>>> {
    zstd::stream::read::Decoder::new(published(path)?)
}

fn summary(verb: &str, kind: Kind, members: &[Member]) -> String {
    let m: Vec<String> = members.iter().map(|m| format!("{} {}", m.name, m.size)).collect();
    format!("{verb} {} unit: {}", kind.name(), m.join(", "))
}

fn encode(kind: &str, dir: &str, file: Option<&str>) -> io::Result<String> {
    let kind = Kind::from_name(kind).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("{kind} is no kind: places, pois, arcs or cols")))?;
    let mut out = io::stdout().lock();
    transport::encode(kind, Path::new(dir), file, &mut out)?;
    out.flush()?;
    Ok(String::new())
}

/// The members of `unit` in `dir`, moved into place once all decoded.
fn decode(unit: &str, dir: &str) -> io::Result<String> {
    let (kind, members) = transport::decode_into_dir(published(unit)?, Path::new(dir), None)?;
    Ok(summary("decoded", kind, &members))
}

/// A member's bytes compared with a file's as they come.
struct Compare {
    name: String,
    file: BufReader<File>,
    at: u64,
    buf: Vec<u8>,
}

impl Write for Compare {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.buf.resize(b.len(), 0);
        let mut got = 0;
        while got < b.len() {
            match self.file.read(&mut self.buf[got..])? {
                0 => break,
                n => got += n,
            }
        }
        if let Some(i) = self.buf[..got].iter().zip(b).position(|(x, y)| x != y) {
            return Err(io::Error::other(format!("{} differs from the unit's at byte {}", self.name, self.at + i as u64)));
        }
        if got < b.len() {
            return Err(io::Error::other(format!("{} is shorter than the unit's: {} bytes", self.name, self.at + got as u64)));
        }
        self.at += b.len() as u64;
        Ok(b.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn verify(unit: &str, dir: &str) -> io::Result<String> {
    let dir = Path::new(dir);
    let decoded = transport::decode(unit_reader(unit)?, &std::env::temp_dir(), |m| {
        let path = dir.join(&m.name);
        let file = File::open(&path).map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", path.display())))?;
        let len = file.metadata()?.len();
        if len != m.size {
            return Err(io::Error::other(format!("{} is {len} bytes, the unit's {}", m.name, m.size)));
        }
        Ok(Compare { name: m.name.clone(), file: BufReader::new(file), at: 0, buf: Vec::new() })
    })?;
    Ok(summary("verified", decoded.kind, &decoded.members.into_iter().map(|(m, _)| m).collect::<Vec<_>>()))
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let a: Vec<&str> = args.iter().map(String::as_str).collect();
    let done = match a.as_slice() {
        ["encode", kind, dir] => encode(kind, dir, None),
        ["encode", kind, dir, file] => encode(kind, dir, Some(file)),
        ["decode", unit, dir] => decode(unit, dir),
        ["verify", unit, dir] => verify(unit, dir),
        _ => {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };
    match done {
        Ok(line) => {
            if !line.is_empty() {
                println!("{line}");
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("geocoder-transport: {e}");
            ExitCode::FAILURE
        }
    }
}
