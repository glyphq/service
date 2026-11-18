use qubic::messages::{LogEvent, Tick, TickData, Transaction};

#[derive(Debug, Clone)]
pub struct VerifiedTick {
    pub head: node::Head,
    pub tick: Tick,
    pub tick_data: TickData,
    pub votes: Vec<Tick>,
    pub transactions: Vec<Transaction>,
    pub logs: Vec<LogEvent>,
    pub verification: Verification,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verification {
    Quorum,
    Data,
    State,
}

impl Verification {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Quorum => "quorum",
            Self::Data => "data",
            Self::State => "state",
        }
    }
}
