use std::fs::File;
use std::io;
use std::path::Path;

use byteorder::{WriteBytesExt, LE};

use crate::error::Result;
use crate::util::create_file;

/// Wraps any writer and records the current position in the writer.
///
/// The position recorded always corresponds to the position that the next
/// byte would be written to.
#[derive(Clone, Debug)]
pub struct CursorWriter<W> {
    wtr: W,
    pos: usize,
}

impl CursorWriter<io::BufWriter<File>> {
    /// Create a new cursor writer that will write to a file at the given path.
    /// The file is truncated before writing.
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = create_file(path)?;
        Ok(CursorWriter::new(io::BufWriter::new(file)))
    }
}

impl<W: io::Write> CursorWriter<W> {
    /// Wrap the given writer with a counter.
    pub fn new(wtr: W) -> CursorWriter<W> {
        CursorWriter {
            wtr: wtr,
            pos: 0,
        }
    }

    /// Return the current position of this writer.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Write a u16LE.
    pub fn write_u16(&mut self, n: u16) -> io::Result<()> {
        WriteBytesExt::write_u16::<LE>(self, n)?;
        Ok(())
    }

    /// Write a u32LE.
    pub fn write_u32(&mut self, n: u32) -> io::Result<()> {
        WriteBytesExt::write_u32::<LE>(self, n)?;
        Ok(())
    }

    /// Write a u64LE.
    pub fn write_u64(&mut self, n: u64) -> io::Result<()> {
        WriteBytesExt::write_u64::<LE>(self, n)?;
        Ok(())
    }
}

impl<W: io::Write> io::Write for CursorWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.wtr.write(buf)?;
        self.pos += n;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.wtr.flush()
    }
}
