use serde::{Deserialize, Serialize};

use crate::outputs::{BuildOutput, OutputDestination};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NatsOutputConfig {
    pub urls: String,
    pub subject: String,
}

impl BuildOutput for NatsOutputConfig {}
pub struct NatsOutput {
    urls: String,
    subject: String,
}

impl OutputDestination for NatsOutput {}
