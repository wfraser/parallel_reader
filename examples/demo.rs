use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::process::exit;
use thread_chunked_reader::ThreadChunkedReader;

struct Args {
    num_threads: usize,
    chunk_size: usize,
    file_path: PathBuf,
}

fn parse_args() -> Option<Args> {
    let mut args = std::env::args_os();
    args.next()?;
    let num_threads = args.next()?.to_string_lossy().parse().ok()?;
    let chunk_size = args.next()?.to_string_lossy().parse().ok()?;
    let file_path = PathBuf::from(args.next()?);
    if args.next().is_some() {
        return None;
    }
    Some(Args { num_threads, chunk_size, file_path })
}

fn main() {
    let args = if let Some(args) = parse_args() { args } else {
        eprintln!("Usage: {} <num_threads> <chunk_size> <file_path>",
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

    let mut reader = ThreadChunkedReader::new(file, args.chunk_size, args.num_threads);
    let result = reader.process_chunks(|offset, data| {
        println!("at {}, {} bytes", offset, data.len());
        if offset == 224 {
            return Err(io::Error::new(io::ErrorKind::Other, "oops"));
        }
        std::thread::sleep(std::time::Duration::from_secs(1));
        Ok(())
    });

    println!("final result: {:?}", result);
}
