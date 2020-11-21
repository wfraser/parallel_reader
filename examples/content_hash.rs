//! This example generates a ["content
//! hash"](https://www.dropbox.com/developers/reference/content-hash) for a file. A content hash is
//! made by hashing a file 4 MiB at a time, concatenating those hashes, and then taking the hash of
//! that. It uses the SHA256 hash.
//!
//! This example shows how to use this crate to do do that hashing in parallel on an arbitrary
//! number of threads.

use ring::digest::{digest, Context, Digest, SHA256};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;
use std::process::exit;
use std::sync::{Arc, Mutex};
use parallel_reader::read_stream_and_process_chunks_in_parallel;

const BLOCK_SIZE: usize = 4 * 1024 * 1024;

struct Args {
    num_threads: usize,
    file_path: PathBuf,
}

fn parse_args() -> Option<Args> {
    let mut args = std::env::args_os();
    args.next()?;
    let num_threads = args.next()?.to_string_lossy().parse().ok()?;
    let file_path = PathBuf::from(args.next()?);
    if args.next().is_some() {
        return None;
    }
    Some(Args { num_threads, file_path })
}

struct State {
    blocks: BTreeMap<u64, Digest>,
    next_offset: u64,
    overall_hash: Context,
    incomplete_block_offset: Option<u64>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            blocks: BTreeMap::new(),
            next_offset: 0,
            overall_hash: Context::new(&SHA256),
            incomplete_block_offset: None,
        }
    }
}

impl State {
    /// Consume sequential blocks in the internal hash buffer and update the overall hash.
    pub fn update_overall_hash(&mut self) {
        loop {
            let key = self.next_offset;
            if let Some(hash) = self.blocks.remove(&key) {
                self.overall_hash.update(hash.as_ref());
                self.next_offset += BLOCK_SIZE as u64;
            } else {
                break;
            }
        }
    }
}

fn main() {
    let args = if let Some(args) = parse_args() { args } else {
        eprintln!("Usage: {} <num_threads> <file_path>",
            std::env::args().next().unwrap());
        exit(1);
    };

    let file = match File::open(&args.file_path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("failed to open {:?}: {}", args.file_path, e);
            exit(2);
        }
    };


    let state = Arc::new(Mutex::new(State::default()));
    let thread_state = state.clone();
    let result = read_stream_and_process_chunks_in_parallel(file, BLOCK_SIZE, args.num_threads,
        Arc::new(move |offset, data: &[u8]| -> Result<(), String> {
            println!("hashing block at {:#x}: {:#x} bytes", offset, data.len());

            let block_hash = digest(&SHA256, data);
            let mut state = thread_state.lock().unwrap();

            // Only the last block in the stream can be smaller than the full block size.
            if let Some(other_offset) = state.incomplete_block_offset {
                // Check where the other one is; if it's after this, it might be okay because it
                // might be the last block in the stream.
                if other_offset < offset {
                    return Err(format!("got incomplete block mid-stream at {:#x}", other_offset));
                }
            }
            if data.len() != BLOCK_SIZE {
                if let Some(other_offset) = state.incomplete_block_offset {
                    return Err(format!("got incomplete block mid-stream at {:#x}",
                        offset.min(other_offset)));
                }
                state.incomplete_block_offset = Some(offset);
            }

            if offset == state.next_offset {
                state.overall_hash.update(block_hash.as_ref());
                state.next_offset += BLOCK_SIZE as u64;
            } else {
                state.update_overall_hash();
                state.blocks.insert(offset, block_hash);
            }

            Ok(())
        }));
    if let Err(e) = result {
        eprintln!("{}", e);
        exit(3);
    }

    // No other thread should have a copy of the Arc now, so extract the State struct out of the
    // Arc and Mutex so we can call finish() on the hash context.
    let mut state = match Arc::try_unwrap(state) {
        Ok(state) => state,
        Err(_) => panic!(),
    }.into_inner().unwrap();

    state.update_overall_hash();
    assert!(state.blocks.is_empty(), "all blocks should be incorporated in the overall hash now");
    println!("{:?}", state.overall_hash.finish());
}
