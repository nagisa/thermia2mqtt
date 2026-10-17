use crate::registers::{Register, Space, Value};
use std::collections::{BTreeMap, BTreeSet};

/// What we know about the contents of the registers of the heat pump.
///
/// Registers that we haven't read yet are not present.
#[derive(Default)]
pub struct Snapshot {
    values: [BTreeMap<u16, u16>; 4],
    /// The addresses that the heat pump has been found to not have anything at.
    unreadable: [BTreeSet<u16>; 4],
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

    /// Record that the heat pump has nothing at this address. Returns `true` if this is news.
    pub(crate) fn set_unreadable(&mut self, space: Space, address: u16) -> bool {
        self.unreadable[space.index()].insert(address)
    }

    /// Stop considering any of the addresses unreadable, returning what they were.
    pub(crate) fn forget_unreadable(&mut self) -> Vec<(Space, u16)> {
        let mut forgotten = Vec::new();
        for space in Space::ALL {
            let addresses = std::mem::take(&mut self.unreadable[space.index()]);
            forgotten.extend(addresses.into_iter().map(|address| (space, address)));
        }
        forgotten
    }

    /// The heat pump has no register at this address, and there is no point in asking for it.
    pub fn is_unreadable(&self, register: Register) -> bool {
        self.unreadable[register.space().index()].contains(&register.address())
    }

    /// All of the addresses known to be unreadable in the space.
    pub(crate) fn unreadable_addresses(&self, space: Space) -> &BTreeSet<u16> {
        &self.unreadable[space.index()]
    }

    /// The registers that we do not know the fate of yet: neither read, nor found to be unreadable.
    pub fn unresolved(&self, registers: impl IntoIterator<Item = Register>) -> Vec<Register> {
        registers.into_iter().filter(|&r| !self.contains(r) && !self.is_unreadable(r)).collect()
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
    }

    #[test]
    fn unreadable_registers_are_resolved() {
        let mut snapshot = Snapshot::new();
        let registers = [Register::holding(22), Register::holding(23), Register::input(22)];
        assert_eq!(snapshot.unresolved(registers).len(), 3);
        snapshot.set(Space::Holding, 22, 1);
        assert!(snapshot.set_unreadable(Space::Holding, 23));
        assert!(!snapshot.set_unreadable(Space::Holding, 23));
        // Unreadable in one space does not say anything about the others.
        assert_eq!(snapshot.unresolved(registers), vec![Register::input(22)]);
        assert!(snapshot.is_unreadable(Register::holding(23)));
        assert!(!snapshot.is_unreadable(Register::input(22)));

        let mut forgotten = snapshot.forget_unreadable();
        forgotten.sort();
        assert_eq!(forgotten, vec![(Space::Holding, 23)]);
        assert!(!snapshot.is_unreadable(Register::holding(23)));
        assert_eq!(snapshot.unresolved(registers).len(), 2);
    }
}
