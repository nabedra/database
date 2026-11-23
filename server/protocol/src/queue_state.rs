use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestState {
    Queued,
    Running,
    Committing,
    Reconciling,
    Succeeded,
    Failed,
    Abandoned,
}
impl RequestState {
    #[inline]
    pub fn terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Abandoned)
    }
}
