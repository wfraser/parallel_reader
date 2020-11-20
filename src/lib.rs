use std::cell::RefCell;
use std::io::{self, Read};
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use threadpool::ThreadPool;

pub struct ThreadChunkedReader<R> {
    threadpool: ThreadPool,
    chunk_size: usize,
    reader: Arc<Mutex<(u64, R)>>,
}

impl<R: Read + Send + 'static> ThreadChunkedReader<R> {
    pub fn new(reader: R, chunk_size: usize, num_threads: usize) -> Self {
        Self {
            threadpool: ThreadPool::new(num_threads),
            chunk_size,
            reader: Arc::new(Mutex::new((0, reader))),
        }
    }

    pub fn process_chunks(
        &mut self,
        f: Arc<impl Fn(u64, &[u8]) -> io::Result<()> + Send + Sync + 'static>,
    ) -> io::Result<()> {
        // Each worker thread gets its own read buffer, which gets reused for each read.
        thread_local! {
            pub static BUF: RefCell<Vec<u8>> = RefCell::new(vec![]);
        }

        // Each read operation sends a message with its result, which indicates any I/O errors, or
        // is true/false for whether the end of the file has been reached yet or not.
        // This gets checked after every chunk is read, before the user's job is run.
        let (read_tx, read_rx) = mpsc::channel::<io::Result<bool>>();

        // If a job returns an error result, it will be sent to this channel. This is used to stop
        // reading early if any job fails.
        let (job_tx, job_rx) = mpsc::channel::<io::Result<()>>();

        let loop_result = loop {
            let thread_read_tx = read_tx.clone();
            let thread_job_tx = job_tx.clone();
            let thread_reader_mutex = self.reader.clone();
            let chunk_size = self.chunk_size;
            let f = f.clone();

            self.threadpool.execute(move || {
                let read_result = {
                    let mut guard = thread_reader_mutex.lock().unwrap();
                    let (ref mut offset, ref mut reader) = *guard;

                    let read_result = BUF.with(move |cell| {
                        let mut buf = cell.borrow_mut();
                        buf.resize(chunk_size, 0);

                        match reader.read(&mut buf) {
                            Ok(0) => {
                                // Done reading. Tell the main loop to stop.
                                thread_read_tx.send(Ok(false)).unwrap();
                                None
                            }
                            Ok(n) => {
                                // We have a chunk. Increment the offset and let the main loop know
                                // to keep going.
                                let job_offset = *offset;
                                *offset += n as u64;
                                thread_read_tx.send(Ok(true)).unwrap();
                                Some((job_offset, n))
                            }
                            Err(e) => {
                                // Read error. Pass the error to the main loop to tell it to stop.
                                thread_read_tx.send(Err(e)).unwrap();
                                None
                            }
                        }
                    });
                    // Unlock the mutex and return the result of the read.
                    read_result
                };

                let (job_offset, nread) = match read_result {
                    Some(value) => value,
                    None => return,
                };

                BUF.with(move |cell| {
                    let buf = cell.borrow();
                    match f(job_offset, &buf[0..nread]) {
                        Ok(()) => (),
                        Err(e) => {
                            // Job returned an error. Pass it to the main loop so it can stop
                            // early.
                            thread_job_tx.send(Err(e)).unwrap();
                        }
                    }
                });
            });

            // Check the read status (blocking).
            match read_rx.recv().unwrap() {
                Ok(true) => (),
                Ok(false) => break Ok(()), // We've reached EOF.
                Err(e) => break Err(e),
            }

            // Check if any job sent anything. They only send if there's an error, so if we get
            // something, stop the loop and pass the error up.
            match job_rx.try_recv() {
                Ok(result) => break result,
                Err(mpsc::TryRecvError::Empty) => (),
                Err(mpsc::TryRecvError::Disconnected) => unreachable!("we hold the sender open"),
            }
        };

        // Loop is finished; wait for outstanding jobs to stop.
        self.threadpool.join();

        if let Err(e) = loop_result {
            // Something stopped the loop prematurely: either a job failed or a read error
            // occurred. Return this error.
            return Err(e);
        }

        // Otherwise, the loop finished, but some job may have failed towards the end so check the
        // channel as well.
        match job_rx.try_recv() {
            Ok(Ok(())) => unreachable!("final job status shouldn't have a non-error value"),
            Ok(Err(e)) => {
                // Some job returned an error.
                Err(e)
            }
            Err(mpsc::TryRecvError::Empty) => {
                // No jobs returned any errors.
                Ok(())
            }
            Err(mpsc::TryRecvError::Disconnected) => unreachable!("we hold the sender open"),
        }
    }
}
