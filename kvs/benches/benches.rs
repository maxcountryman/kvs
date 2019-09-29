#[macro_use]
extern crate criterion;

use criterion::{BatchSize, Criterion};
use rand::rngs::SmallRng;
use rand::Rng;
use rand_core::SeedableRng;
use sled;
use tempfile::TempDir;

use kvs::{KvStore, KvsEngine, SledKvsEngine};

fn bench_set(c: &mut Criterion) {
    let mut group = c.benchmark_group("Set");

    group.bench_function("kvs.set", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut db = KvStore::open(temp_dir.path()).unwrap();
        b.iter(|| {
            for key_i in 1..(1 << 8) {
                db.set(format!("key{}", key_i), "value").unwrap();
            }
        });
    });

    group.bench_function("sled.set", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SledKvsEngine::new(sled::Db::start_default(&temp_dir).unwrap());
        b.iter(|| {
            for key_i in 1..(1 << 8) {
                db.set(format!("key{}", key_i), "value").unwrap();
            }
        });
    });

    group.finish();
}

fn bench_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("Get");

    group.bench_function("kvs.get", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut store = KvStore::open(temp_dir.path()).unwrap();

        for key_i in 1..(1 << 8) {
            store.set(format!("key{}", key_i), "value").unwrap();
        }

        let mut rng = SmallRng::from_seed([0; 16]);
        b.iter(|| {
            store
                .get(format!("key{}", rng.gen_range(1, 1 << 8)))
                .unwrap();
        })
    });

    group.bench_function("sled.get", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut db = SledKvsEngine::new(sled::Db::start_default(&temp_dir).unwrap());

        for key_i in 1..(1 << 8) {
            db.set(format!("key{}", key_i), "value").unwrap();
        }

        let mut rng = SmallRng::from_seed([0; 16]);
        b.iter(|| {
            db.get(format!("key{}", rng.gen_range(1, 1 << 8))).unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, bench_set, bench_get);
criterion_main!(benches);
