use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum ReduceFn {
    Min,
    Max,
    Sum,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Transform {
    Buffer {
        size: usize,
    },
    Reduce {
        reduce_field: String,
        reduce_fn: ReduceFn,
    },
}
