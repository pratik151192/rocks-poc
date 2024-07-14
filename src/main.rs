use rocksdb::{Options, WriteOptions, DB};
use std::fs::OpenOptions;
use std::time::{Instant, Duration};
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use std::io::Write;

fn generate_random_key() -> Vec<u8> {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(100)
        .collect()
}

fn generate_random_chunk(size: usize) -> Vec<u8> {
    (0..size).map(|_| thread_rng().gen::<u8>()).collect()
}

fn benchmark_chunk_size(db: &DB, chunk: &[u8], write_options: &WriteOptions, duration: Duration) -> (f64, f64, Duration, Duration, Duration) {
    let mut latencies = Vec::new();
    let total_start = Instant::now();
    let mut total_bytes_written = 0;
    let mut total_writes = 0;

    while total_start.elapsed() < duration {
        let key = generate_random_key();
        let start = Instant::now();
        db.put_opt(&key, chunk, write_options).unwrap();
        let duration = start.elapsed();
        latencies.push(duration);
        total_bytes_written += chunk.len();
        total_writes += 1;
    }

    let total_duration = total_start.elapsed();
    let throughput_mib = (total_bytes_written as f64 / (1024.0 * 1024.0)) / total_duration.as_secs_f64();
    let writes_per_second = total_writes as f64 / total_duration.as_secs_f64();

    latencies.sort();
    let p50 = latencies[latencies.len() / 2];
    let p90 = latencies[(latencies.len() as f64 * 0.9) as usize];
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];

    (throughput_mib, writes_per_second, p50, p90, p99)
}

fn run_experiment(chunk_sizes: &[usize], direct_io: bool, sync: bool, duration: Duration) -> Vec<(usize, f64, f64, u128, u128, u128)> {
    let path = "db/";
    let mut results = Vec::new();

    let mut write_options = WriteOptions::default();
    write_options.set_sync(sync);

    for &chunk_size in chunk_sizes {
        let _ = DB::destroy(&Options::default(), path);
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_use_direct_io_for_flush_and_compaction(direct_io);
        let db = DB::open(&options, path).unwrap();

        let chunk = generate_random_chunk(chunk_size);
        let (throughput, wps, p50, p90, p99) = benchmark_chunk_size(&db, &chunk, &write_options, duration);
        
        results.push((chunk_size, throughput, wps, p50.as_micros(), p90.as_micros(), p99.as_micros()));

        println!("Chunk size: {} bytes, Direct I/O: {}, Sync: {}", chunk_size, direct_io, sync);
        println!("Throughput: {:.2} MiB/second", throughput);
        println!("Writes per second: {:.2}", wps);
        println!("p50 latency: {:?}", p50);
        println!("p90 latency: {:?}", p90);
        println!("p99 latency: {:?}", p99);
        println!("--------------------");
    }

    results
}

fn rocks_db_benchmark() -> Result<(), Box<dyn std::error::Error>> {
    let chunk_sizes = vec![4*1024, 8*1024, 16*1024, 32*1024, 64*1024, 512*1024, 1024*1024];
    let duration = Duration::from_secs(300); // 1 minute per experiment

    let mut csv_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open("benchmark_results.csv")?;

    writeln!(csv_file, "Direct I/O,Sync,Chunk Size (bytes),Throughput (MiB/s),Writes per Second,p50 Latency (µs),p90 Latency (µs),p99 Latency (µs)")?;

    for direct_io in [true, false] {
        for sync in [false, true] {
            let results = run_experiment(&chunk_sizes, direct_io, sync, duration);
            
            for (chunk_size, throughput, wps, p50, p90, p99) in results {
                writeln!(csv_file, "{},{},{},{:.2},{:.2},{},{},{}",
                         direct_io, sync, chunk_size, throughput, wps, p50, p90, p99)?;
            }
        }
    }

    println!("Results have been written to benchmark_results.csv");

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    rocks_db_benchmark()
}