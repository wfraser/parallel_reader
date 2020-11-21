`parallel_reader`
=================

A utility for reading from a file (or any `Read` stream) and processing it by chunks, in parallel.

This is useful if reading is not a bottleneck, and you have something slow to do with it that is
easily parallelizable.

Examples might be:
* hashing
* compression
* sending chunks over a network

This crate provides a function, `read_stream_and_process_chunks_in_parallel` that lets you give a
`Read` stream, then specify a chunk size and number of threads, and some processing function, and
it'll take care of reading the stream and assigning threads to work on chunks of it.

Your function can also return an error, and it'll stop the processing of the file early and return
the error to you, including the chunk offset that it was on when it errored.

For now, this is only using synchronous, blocking I/O, but maybe in the future I'll add another
function that uses async streams and runs futures in parallel. P/Rs are welcome.
