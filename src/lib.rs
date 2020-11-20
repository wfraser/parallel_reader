use std::io::{self, Read};
use std::sync::{Arc, Mutex};
use std::sync::mpsc;
use threadpool::ThreadPool;

pub struct ThreadChunkedReader<R> {
    threadpool: ThreadPool,
    chunk_size: usize,
    reader: R,
}

impl<R: Read> ThreadChunkedReader<R> {
    pub fn new(reader: R, chunk_size: usize, num_threads: usize) -> Self {
        Self {
            threadpool: ThreadPool::new(num_threads),
            chunk_size,
            reader,
        }
    }

    pub fn process_chunks(
        &mut self,
        f: Arc<impl Fn(u64, &[u8]) -> io::Result<()> + Send + Sync + 'static>,
    ) -> io::Result<()> {
        // TODO: have a pool of pre-allocated, reusable buffers

        // Channel for sending work as (offset, data) pairs to the worker threads.
        // It's bounded by the number of workers, to ensure we don't read ahead of the work too
        // far.
        let (work_tx, work_rx) = mpsc::sync_channel::<(u64, Vec<u8>)>(self.threadpool.max_count());

        // If a job returns an error result, it will be sent to this channel. This is used to stop
        // reading early if any job fails.
        let (job_tx, job_rx) = mpsc::channel::<io::Result<()>>();

        // Start up workers.
        // TODO: get rid of threadpool and just manage threads ourselves

        let work_rx = Arc::new(Mutex::new(work_rx));
        for _ in 0 .. self.threadpool.max_count() {
            let thread_work_rx = work_rx.clone();
            let thread_job_tx = job_tx.clone();
            let f = f.clone();

            self.threadpool.execute(move || {
                loop {
                    let (offset, data) = {
                        let rx = thread_work_rx.lock().unwrap();

                        match rx.recv() {
                            Ok(result) => result,
                            Err(_) => {
                                // Sender end of the channel disconnected. Main thread must be done
                                // and waiting for us.
                                return;
                            }
                        }
                    };

                    if let Err(e) = f(offset, &data) {
                        // Job returned an error. Pass it to the main loop so it can stop early.
                        thread_job_tx.send(Err(e)).unwrap();
                    }
                }
            });
        }

        drop(work_rx);

        // Read the file in chunks and pass work to worker threads.
        let mut offset = 0u64;
        let loop_result = loop {
            // Check if any job sent anything. They only send if there's an error, so if we get
            // something, stop the loop and pass the error up.
            match job_rx.try_recv() {
                Ok(result) => break result,
                Err(mpsc::TryRecvError::Empty) => (),
                Err(mpsc::TryRecvError::Disconnected) => unreachable!("we hold the sender open"),
            }

            let mut buf = vec![0u8; self.chunk_size];
            match self.reader.read(&mut buf) {
                Ok(0) => {
                    break Ok(());
                }
                Ok(n) => {
                    buf.truncate(n);
                    work_tx.send((offset, buf)).unwrap();
                    offset += n as u64;
                }
                Err(e) => {
                    break Err(e);
                }
            }
        };

        drop(work_tx);

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
