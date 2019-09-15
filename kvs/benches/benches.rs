#[macro_use]
extern crate criterion;

use criterion::{BatchSize, Criterion};
use rand::Rng;
use rand::rngs::SmallRng;
use rand_core::SeedableRng;
use sled;
use tempfile::TempDir;

use kvs::{KvStore, KvsEngine, SledKvsEngine};

fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("KvsEngine::set");

    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                KvStore::open(temp_dir.path()).unwrap()
            },
            |mut db| {
                for i in 1..(1 << 12) {
                    db.set(format!("key{}", i), "value").unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                SledKvsEngine::new(sled::Db::start_default(&temp_dir).unwrap())
            },
            |mut db| {
                for i in 1..(1 << 12) {
                    db.set(format!("key{}", i), "value").unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("KvsEngine::get");

    group.bench_function("kvs", |b| {
        for i in [8, 12, 16, 20].iter() {
            let temp_dir = TempDir::new().unwrap();
            let mut store = KvStore::open(temp_dir.path()).unwrap();

            for key_i in 1..(1 << i) {
                store.set(format!("key{}", key_i), "value").unwrap();
            }

            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1, 1 << i)))
                    .unwrap();
            })
        }
    });

    group.bench_function("sled", |b| {
        for i in [8, 12, 16, 20].iter() {
            let temp_dir = TempDir::new().unwrap();
            let mut db = SledKvsEngine::new(sled::Db::start_default(&temp_dir).unwrap());

            for key_i in 1..(1 << i) {
                db.set(format!("key{}", key_i), "value").unwrap();
            }

            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                db.get(format!("key{}", rng.gen_range(1, 1 << i))).unwrap();
            })
        }
    });

    group.finish();
}

criterion_group!(benches, bench_set, bench_get);
criterion_main!(benches);
