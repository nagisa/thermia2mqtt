use crate::registers::{Register, Space, Value};
use std::collections::BTreeMap;

/// What we know about the contents of the registers of the heat pump.
///
/// Registers that we haven't read yet are not present.
#[derive(Default)]
pub struct Snapshot {
    values: [BTreeMap<u16, u16>; 4],
}

impl Snapshot {
    pub fn new() -> Self {
        Self::default()
    }

    /// The word last read from the register at the specified address.
    pub fn raw(&self, space: Space, address: u16) -> Option<u16> {
        self.values[space.index()].get(&address).copied()
    }

    pub fn value(&self, register: Register) -> Option<Value> {
        let raw = self.raw(register.space(), register.address())?;
        Some(Value::new(register, raw))
    }

    pub fn contains(&self, register: Register) -> bool {
        self.raw(register.space(), register.address()).is_some()
    }

    /// Record a newly read word. Returns `true` if the stored value has changed.
    pub(crate) fn set(&mut self, space: Space, address: u16, raw: u16) -> bool {
        self.values[space.index()].insert(address, raw) != Some(raw)
    }

    /// The registers out of those specified that have not been read yet.
    pub fn missing(&self, registers: impl IntoIterator<Item = Register>) -> Vec<Register> {
        registers.into_iter().filter(|&r| !self.contains(r)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spaces_do_not_collide() {
        let mut snapshot = Snapshot::new();
        assert!(snapshot.set(Space::Input, 22, 1));
        assert!(snapshot.set(Space::Holding, 22, 2));
        assert!(!snapshot.set(Space::Holding, 22, 2));
        assert!(snapshot.set(Space::Holding, 22, 3));
        assert_eq!(snapshot.raw(Space::Input, 22), Some(1));
        assert_eq!(snapshot.value(Register::holding(22)).map(|v| v.raw()), Some(3));
        assert_eq!(snapshot.missing([Register::holding(22), Register::holding(23)]).len(), 1);
    }
}
