//! Uses [`Mmap`](memmap2) to map a file into memory with kernel support.
//!
//! Choose this implementation if:
//!
//! 1. Your platform supports memory maps.
//! 2. The input data is in a file or comes from standard input:
//!   a) if from a file, then you can guarantee that the file is not going to be modified
//!      in or out of process while the input is alive;
//!   b) if from stdin, then that the input lives in memory (for example comes via a pipe);
//!      input from a tty is not memory-mappable.
//!
//! ## Performance characteristics
//!
//! A memory map is by far the fastest way to process a file. For some queries it is faster
//! by an order of magnitude to execute the query on a memory map than it is to simply read the
//! file into main memory.

use std::fs::File;

use super::{error::InputError, in_slice, Input, InputBlockIterator, MAX_BLOCK_SIZE};
use crate::{query::JsonString, FallibleIterator};
use memmap2::{Mmap, MmapOptions};

/// Input wrapping a memory mapped file.
pub struct MmapInput {
    mmap: Mmap,
}

impl MmapInput {
    /// Map a file to memory.
    ///
    /// # Safety
    ///
    /// This operation is inherently unsafe, since the file can be modified
    /// in or out of process. See [Mmap documentation](https://docs.rs/memmap2/latest/memmap2/struct.Mmap.html).
    ///
    /// # Errors
    ///
    /// Calling mmap might result in an IO error.
    #[inline]
    pub unsafe fn map_file(file: &File) -> Result<Self, InputError> {
        let file_len = file.metadata()?.len() as usize;

        let rem = file_len % MAX_BLOCK_SIZE;
        let pad = if rem == 0 { 0 } else { MAX_BLOCK_SIZE - rem };

        let mmap = MmapOptions::new().len(file_len + pad).map(file);

        match mmap {
            Ok(mmap) => Ok(Self { mmap }),
            Err(err) => Err(err.into()),
        }
    }
}

impl Input for MmapInput {
    type BlockIterator<'a, const N: usize> = MmapBlockIterator<'a, N>;

    #[inline(always)]
    fn iter_blocks<const N: usize>(&self) -> Self::BlockIterator<'_, N> {
        MmapBlockIterator::new(&self.mmap)
    }

    #[inline]
    fn seek_backward(&self, from: usize, needle: u8) -> Option<usize> {
        in_slice::seek_backward(&self.mmap, from, needle)
    }

    #[inline]
    fn seek_non_whitespace_forward(&self, from: usize) -> Result<Option<(usize, u8)>, InputError> {
        Ok(in_slice::seek_non_whitespace_forward(&self.mmap, from))
    }

    #[inline]
    fn seek_non_whitespace_backward(&self, from: usize) -> Option<(usize, u8)> {
        in_slice::seek_non_whitespace_backward(&self.mmap, from)
    }

    #[inline]
    #[cfg(feature = "head-skip")]
    fn find_member(&self, from: usize, label: &JsonString) -> Result<Option<usize>, InputError> {
        Ok(in_slice::find_member(&self.mmap, from, label))
    }

    #[inline]
    fn is_member_match(&self, from: usize, to: usize, label: &JsonString) -> bool {
        in_slice::is_member_match(&self.mmap, from, to, label)
    }
}

/// Iterator over blocks of [`BorrowedBytes`] of size exactly `N`.
pub struct MmapBlockIterator<'a, const N: usize> {
    input: &'a [u8],
    idx: usize,
}

impl<'a, const N: usize> MmapBlockIterator<'a, N> {
    #[must_use]
    #[inline(always)]
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { input: bytes, idx: 0 }
    }
}

impl<'a, const N: usize> FallibleIterator for MmapBlockIterator<'a, N> {
    type Item = &'a [u8];
    type Error = InputError;

    #[inline]
    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        if self.idx >= self.input.len() {
            Ok(None)
        } else {
            let block = &self.input[self.idx..self.idx + N];
            self.idx += N;

            Ok(Some(block))
        }
    }
}

impl<'a, const N: usize> InputBlockIterator<'a, N> for MmapBlockIterator<'a, N> {
    type Block = &'a [u8];

    #[inline(always)]
    fn offset(&mut self, count: isize) {
        assert!(count >= 0);
        self.idx += count as usize * N;
    }
}
