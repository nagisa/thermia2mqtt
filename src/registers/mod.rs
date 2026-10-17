//! The registers of the heat pump as documented in the Thermia "Modbus protocol for Mega, Genesis
//! platform" guide.
//!
//! The guide splits the registers up into four address spaces, each with its own function codes.
//! Addresses are only unique within a single space. The addresses used throughout this crate are the
//! zero-based "Address" column of the guide, i.e. what is sent over the wire. (The "De Facto
//! address" column is that plus 1, 10001, 30001 or 40001.)

mod table;

/// One of the four address spaces of Modbus.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Space {
    /// Read/write booleans (function codes 1, 5, 15).
    Coil,
    /// Read-only booleans (function code 2).
    DiscreteInput,
    /// Read-only 16-bit words (function code 4).
    Input,
    /// Read/write 16-bit words (function codes 3, 6, 16).
    Holding,
}

impl Space {
    pub const ALL: [Space; 4] = [Space::Coil, Space::DiscreteInput, Space::Input, Space::Holding];

    pub const fn index(self) -> usize {
        self as usize
    }

    /// Whether the registers in this space hold single bits (as opposed to 16-bit words.)
    pub const fn is_bit(self) -> bool {
        matches!(self, Space::Coil | Space::DiscreteInput)
    }

    pub const fn is_writable(self) -> bool {
        matches!(self, Space::Coil | Space::Holding)
    }

    pub const fn name(self) -> &'static str {
        match self {
            Space::Coil => "coil",
            Space::DiscreteInput => "discrete-input",
            Space::Input => "input",
            Space::Holding => "holding",
        }
    }

    /// The number added to a zero-based address to obtain the "de facto" address.
    pub const fn de_facto_base(self) -> u32 {
        match self {
            Space::Coil => 1,
            Space::DiscreteInput => 10001,
            Space::Input => 30001,
            Space::Holding => 40001,
        }
    }
}

impl std::fmt::Display for Space {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// A register as documented in the guide.
#[derive(Debug)]
pub struct RegisterDef {
    pub space: Space,
    pub address: u16,
    /// The transmitted value is this many times larger than the real value.
    pub scale: u16,
    pub signed: bool,
    /// The unit, if the guide specifies one.
    pub unit: &'static str,
    pub description: &'static str,
}

impl RegisterDef {
    pub fn de_facto_address(&self) -> u32 {
        self.space.de_facto_base() + u32::from(self.address)
    }
}

const fn r(
    space: Space,
    address: u16,
    scale: u16,
    signed: bool,
    unit: &'static str,
    description: &'static str,
) -> RegisterDef {
    RegisterDef { space, address, scale, signed, unit, description }
}

const fn find(space: Space, address: u16) -> Option<&'static RegisterDef> {
    let mut i = 0;
    while i < table::REGISTERS.len() {
        let def = &table::REGISTERS[i];
        if def.space as u8 == space as u8 && def.address == address {
            return Some(def);
        }
        i += 1;
    }
    None
}

/// A reference to one of the registers that are documented in the guide.
#[derive(Copy, Clone)]
pub struct Register {
    def: &'static RegisterDef,
}

impl Register {
    /// Refer to a register that must exist in the guide.
    ///
    /// Fails compilation when used in a const context and the register is not known.
    pub const fn new(space: Space, address: u16) -> Register {
        match find(space, address) {
            Some(def) => Register { def },
            None => panic!("register is not documented in the guide"),
        }
    }

    pub const fn coil(address: u16) -> Register {
        Self::new(Space::Coil, address)
    }

    pub const fn discrete_input(address: u16) -> Register {
        Self::new(Space::DiscreteInput, address)
    }

    pub const fn input(address: u16) -> Register {
        Self::new(Space::Input, address)
    }

    pub const fn holding(address: u16) -> Register {
        Self::new(Space::Holding, address)
    }

    pub fn lookup(space: Space, address: u16) -> Option<Register> {
        find(space, address).map(|def| Register { def })
    }

    /// All the documented registers.
    pub fn all() -> impl Iterator<Item = Register> {
        table::REGISTERS.iter().map(|def| Register { def })
    }

    pub const fn space(self) -> Space {
        self.def.space
    }

    pub const fn address(self) -> u16 {
        self.def.address
    }

    pub const fn def(self) -> &'static RegisterDef {
        self.def
    }
}

impl PartialEq for Register {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for Register {}

impl PartialOrd for Register {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Register {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(&other.key())
    }
}

impl std::hash::Hash for Register {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key().hash(state)
    }
}

impl Register {
    fn key(&self) -> (Space, u16) {
        (self.def.space, self.def.address)
    }
}

impl std::fmt::Debug for Register {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}[{}] ({})", self.def.space, self.def.address, self.def.description)
    }
}

/// The word read out of a register together with the knowledge of how to interpret it.
#[derive(Copy, Clone, Debug)]
pub struct Value {
    raw: u16,
    register: Register,
}

impl Value {
    pub fn new(register: Register, raw: u16) -> Value {
        Value { raw, register }
    }

    pub fn register(self) -> Register {
        self.register
    }

    pub fn raw(self) -> u16 {
        self.raw
    }

    pub fn as_bool(self) -> bool {
        self.raw != 0
    }

    /// The value with the sign taken into account, but not scaled.
    pub fn as_i32(self) -> i32 {
        if self.register.def.signed { i32::from(self.raw as i16) } else { i32::from(self.raw) }
    }

    /// The value with the scale applied.
    pub fn as_f64(self) -> f64 {
        f64::from(self.as_i32()) / f64::from(self.register.def.scale)
    }
}

/// Convert a real value into what should be written into the register.
///
/// Returns `None` if the value cannot be represented.
pub fn to_raw(register: Register, value: f64) -> Option<u16> {
    let def = register.def;
    let scaled = (value * f64::from(def.scale)).round();
    if !scaled.is_finite() {
        return None;
    }
    if def.signed {
        (f64::from(i16::MIN)..=f64::from(i16::MAX)).contains(&scaled).then(|| scaled as i16 as u16)
    } else {
        (0.0..=f64::from(u16::MAX)).contains(&scaled).then(|| scaled as u16)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_are_unique() {
        let mut seen = std::collections::BTreeSet::new();
        for def in table::REGISTERS {
            assert!(seen.insert((def.space, def.address)), "{:?} is duplicated", def);
            assert!(matches!(def.scale, 1 | 10 | 100), "{def:?} has an odd scale");
            assert!(!def.description.is_empty(), "{def:?} has no description");
            assert!(!def.description.contains("  "), "{def:?} has extra spaces");
        }
    }

    #[test]
    fn lookup_matches_guide() {
        let outdoor = Register::input(13);
        assert_eq!(outdoor.def().description, "Outdoor temperature");
        assert_eq!(outdoor.def().de_facto_address(), 30014);
        let start = Register::holding(22);
        assert_eq!(start.def().de_facto_address(), 40023);
        assert_eq!(Register::coil(9).def().de_facto_address(), 10);
        assert!(Register::lookup(Space::Input, 0).is_none());
    }

    #[test]
    fn negative_numbers() {
        // Example from the guide: -5 °C with a scale of 100 reads as 65036.
        let register = Register::holding(69);
        assert_eq!(Value::new(register, 65036).as_f64(), -5.0);
        assert_eq!(to_raw(register, -5.0), Some(65036));
        assert_eq!(to_raw(register, 400.0), None);
    }

    #[test]
    fn thresholds_can_be_negative() {
        // 65530 is what -6 looks like.
        let threshold = Register::holding(75);
        assert_eq!(Value::new(threshold, 65530).as_i32(), -6);
        assert_eq!(to_raw(threshold, -6.0), Some(65530));
    }

    #[test]
    fn unsigned_values_do_not_wrap() {
        // "Compressor operating hours (LSB)" is a strict positive number up to 65535.
        let hours = Register::input(49);
        assert_eq!(Value::new(hours, 65535).as_i32(), 65535);
        assert_eq!(to_raw(hours, -1.0), None);
    }
}
