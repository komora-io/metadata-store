use std::num::NonZeroU64;

use inline_array::InlineArray;
use metadata_store::MetadataStore;

const BATCHES: u64 = 1024;
const N_PER_BATCH: u64 = 64 * 1024;

fn main() {
    env_logger::init();

    let (db, recovered) = MetadataStore::recover("timing_test").unwrap();

    dbg!(recovered.len());

    for (k, v, user_data) in recovered {
        assert_eq!(k, v.get());
        assert_eq!(k, u64::from_le_bytes((*user_data).try_into().unwrap()));
    }

    for i in 0..BATCHES {
        let mut batch = Vec::with_capacity(N_PER_BATCH as usize);
        for j in 1..=N_PER_BATCH {
            let k = (i * N_PER_BATCH) + j;
            let v = Some((
                NonZeroU64::new(k).unwrap(),
                InlineArray::from(&k.to_le_bytes()),
            ));
            batch.push((k, v));
        }

        db.insert_batch(batch).unwrap();
    }

    db.shutdown();
}
