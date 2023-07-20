use u64db::U64Db;

const BATCHES: u64 = 1024;
const N_PER_BATCH: u64 = 64 * 1024;

fn main() {
    env_logger::init();

    let db = U64Db::recover("timing_test").unwrap();

    dbg!(db.get(1));

    for i in 0..BATCHES {
        let mut batch = Vec::with_capacity(N_PER_BATCH as usize);
        for j in 0..N_PER_BATCH {
            let k = (i * N_PER_BATCH) + j;
            let v = k;
            batch.push((k, v));
        }

        db.insert_batch(batch).unwrap();
    }
}
