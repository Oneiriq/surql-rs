//! Criterion benchmarks for `surql::types` primitives.
//!
//! Run with `cargo bench --bench types`.

#![allow(missing_docs)]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use surql::types::{eq, gt, RecordID};

fn bench_record_id_new(c: &mut Criterion) {
    c.bench_function("record_id::new simple", |b| {
        b.iter(|| {
            let id = RecordID::<()>::new(black_box("user"), black_box("alice")).unwrap();
            black_box(id);
        });
    });

    c.bench_function("record_id::new complex", |b| {
        b.iter(|| {
            let id =
                RecordID::<()>::new(black_box("outlet"), black_box("alaskabeacon.com")).unwrap();
            black_box(id);
        });
    });

    c.bench_function("record_id::new int", |b| {
        b.iter(|| {
            let id = RecordID::<()>::new(black_box("post"), black_box(42_i64)).unwrap();
            black_box(id);
        });
    });
}

fn bench_record_id_parse(c: &mut Criterion) {
    c.bench_function("record_id::parse simple", |b| {
        b.iter(|| {
            let id = RecordID::<()>::parse(black_box("user:alice")).unwrap();
            black_box(id);
        });
    });

    c.bench_function("record_id::parse complex", |b| {
        b.iter(|| {
            let id = RecordID::<()>::parse(black_box("outlet:<alaskabeacon.com>")).unwrap();
            black_box(id);
        });
    });
}

fn bench_record_id_display(c: &mut Criterion) {
    let id = RecordID::<()>::new("user", "alice").unwrap();
    c.bench_function("record_id::display", |b| {
        b.iter(|| {
            let s = black_box(&id).to_string();
            black_box(s);
        });
    });
}

fn bench_operators(c: &mut Criterion) {
    c.bench_function("operators::eq", |b| {
        b.iter(|| {
            let op = eq(black_box("status"), black_box("active"));
            black_box(op);
        });
    });

    c.bench_function("operators::gt", |b| {
        b.iter(|| {
            let op = gt(black_box("score"), black_box(100));
            black_box(op);
        });
    });
}

criterion_group!(
    benches,
    bench_record_id_new,
    bench_record_id_parse,
    bench_record_id_display,
    bench_operators
);
criterion_main!(benches);
