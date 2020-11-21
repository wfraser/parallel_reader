use std::fs::File;
use std::path::PathBuf;
use std::process::exit;
use std::sync::Arc;
use parallel_reader::read_stream_and_process_chunks_in_parallel;

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

    let result = read_stream_and_process_chunks_in_parallel(file, args.chunk_size, args.num_threads,
        Arc::new(|offset, data: &[u8]| {
            println!("at {}, {} bytes", offset, data.len());
            if offset == 1792 {
                return Err("oops");
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
            Ok(())
        }));

    match result {
        Ok(()) => println!("all succeeded"),
        Err(e) => println!("{}", e),
    }
}
