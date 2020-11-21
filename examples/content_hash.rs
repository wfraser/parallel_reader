use ring::digest::{digest, Context, SHA256};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;
use std::process::exit;
use std::sync::{Arc, Mutex};
use thread_chunked_reader::parallel_chunked_read;

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

    /*
    struct State {
        next_offset: u64,
        blocks: BTreeMap<u64, Vec<u8>>,
    }

    let hashes = Arc::new(Mutex::new(State { next_offset: 0, blocks: BTreeMap::new() }));
    */
    let hashes = Arc::new(Mutex::new(BTreeMap::new()));
    let thread_hashes = hashes.clone();
    let result = parallel_chunked_read(file, 4 * 1024 * 1024, args.num_threads,
        Arc::new(move |offset, data: &[u8]| {
            println!("hashing block at {:#x}", offset);
            let chunk_hash = digest(&SHA256, data);
            let mut hashes = thread_hashes.lock().unwrap();
            hashes.insert(offset, Vec::from(chunk_hash.as_ref()));
            Ok::<(), ()>(())
        }));
    if let Err(thread_chunked_reader::Error::Read(e)) = result {
        eprintln!("read error: {}", e);
        exit(3);
    }

    let hashes = hashes.lock().unwrap();
    println!("got {} hashes; computing overall hash", hashes.len());
    let mut hash_of_hashes = Context::new(&SHA256);
    for h in hashes.values() {
        hash_of_hashes.update(h);
    }

    println!("{:x?}", hash_of_hashes.finish()/*finalize()*/);
}
