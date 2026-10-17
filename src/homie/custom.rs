//! Properties that do not fit the mold of the generic kinds.

use crate::homie::node::{PropertyEntry, PropertyKind};
use crate::homie::value::{
    AggregatePropertyValue, DynPropertyValue, PropertyDescription, PropertyValue,
};
use crate::modbus::Snapshot;
use crate::registers::{Register, Space, Value};
use homie5::device_description::{HomiePropertyDescription, PropertyDescriptionBuilder};
use std::sync::OnceLock;

pub(crate) const STRING_LIST_SCHEMA: &str = r#"{"type":"array","items":{"type":"string"}}"#;
const HEAT_CURVE_SCHEMA: &str = r#"{"type":"array","items":{"type":"array","items":{"type":"number"},"minItems":2,"maxItems":2}}"#;

pub(crate) fn json<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("serializing to JSON cannot fail")
}

pub(crate) fn json_description(schema: &str) -> HomiePropertyDescription {
    PropertyDescriptionBuilder::json().json_schema(schema).build()
}

fn read_only(description: &mut HomiePropertyDescription) {
    description.retained = true;
    description.settable = false;
}

/// A list of strings, published as a JSON array.
pub(crate) struct StringList(Vec<String>);
impl PropertyValue for StringList {
    fn value(&self) -> String {
        json(&self.0)
    }
}
impl PropertyDescription for StringList {
    fn description(_: &PropertyEntry) -> HomiePropertyDescription {
        json_description(STRING_LIST_SCHEMA)
    }
}

/// The points of a heat curve as `[outdoor temperature, supply line temperature]` pairs.
pub(crate) struct HeatCurve(Vec<[f64; 2]>);
impl PropertyValue for HeatCurve {
    fn value(&self) -> String {
        json(&self.0)
    }
}
impl PropertyDescription for HeatCurve {
    fn description(_: &PropertyEntry) -> HomiePropertyDescription {
        json_description(HEAT_CURVE_SCHEMA)
    }
}

/// Which kinds of operation the heat pump is carrying out at the moment, as a JSON array.
///
/// This is a bit field, see footnote 6 in the guide.
pub(crate) struct RunningDemands(u16);

const DEMAND_BITS: &[(u16, &str)] = &[
    (0, "manual-operation"),
    (1, "defrost"),
    (2, "hot-water"),
    (3, "heat"),
    (4, "active-cooling"),
    (5, "pool"),
    (6, "anti-legionella"),
    (7, "passive-cooling"),
    (9, "standby"),
    (10, "no-demand"),
    (11, "off"),
];

impl RunningDemands {
    pub(crate) fn new(bits: Value) -> Result<Self, ()> {
        Ok(Self(bits.raw()))
    }
}
impl PropertyValue for RunningDemands {
    fn value(&self) -> String {
        let active = DEMAND_BITS.iter().filter(|(bit, _)| self.0 & (1 << bit) != 0);
        json(&active.map(|(_, name)| *name).collect::<Vec<_>>())
    }
}
impl PropertyDescription for RunningDemands {
    fn description(_: &PropertyEntry) -> HomiePropertyDescription {
        json_description(STRING_LIST_SCHEMA)
    }
}
impl AggregatePropertyValue for RunningDemands {
    const SETTABLE: bool = false;
}
impl TryFrom<&str> for RunningDemands {
    type Error = ();
    fn try_from(_: &str) -> Result<Self, Self::Error> {
        Err(())
    }
}

/// The names of the alarms that are currently active.
///
/// Every alarm that the guide documents counts, so this does not need to know of them one by one.
pub(crate) struct ActiveAlarmsKind;

struct Alarms {
    registers: Vec<Register>,
    names: Vec<String>,
}

fn alarms() -> &'static Alarms {
    static ALARMS: OnceLock<Alarms> = OnceLock::new();
    ALARMS.get_or_init(|| {
        let mut alarms: Vec<(Register, String)> = Register::all()
            .filter(|r| r.space() == Space::DiscreteInput)
            .filter(|r| {
                let description = r.def().description.to_lowercase();
                description.contains("alarm")
                    // These are covered by the alarm classes.
                    && !description.starts_with("alarm active, class")
                    && !description.starts_with("sum alarm")
            })
            .map(|r| (r, alarm_name(r.def().description)))
            .collect();
        alarms.sort_by_key(|(r, _)| r.address());
        // Some descriptions are only distinguished by the lengthy text we're cutting off.
        let names: Vec<String> = alarms.iter().map(|(_, name)| name.clone()).collect();
        for (register, name) in &mut alarms {
            if names.iter().filter(|n| *n == name).count() > 1 {
                *name = format!("{name} ({})", register.address());
            }
        }
        Alarms {
            registers: alarms.iter().map(|(r, _)| *r).collect(),
            names: alarms.into_iter().map(|(_, n)| n).collect(),
        }
    })
}

fn alarm_name(description: &str) -> String {
    let short = description.split(" - ").next().unwrap_or(description);
    short.trim().trim_end_matches('.').to_string()
}

impl PropertyKind for ActiveAlarmsKind {
    fn registers(&self) -> &[Register] {
        &alarms().registers
    }

    fn value_from_modbus(&self, snapshot: &Snapshot) -> Option<Result<Box<DynPropertyValue>, ()>> {
        let alarms = alarms();
        let mut active = Vec::new();
        for (register, name) in alarms.registers.iter().zip(&alarms.names) {
            match snapshot.value(*register) {
                Some(value) if value.as_bool() => active.push(name.clone()),
                Some(_) => {}
                // An alarm that the heat pump does not have is never active.
                None if snapshot.is_unreadable(*register) => {}
                None => return None,
            }
        }
        Some(Ok(Box::new(StringList(active))))
    }

    fn value_from_homie(&self, _: &str) -> Result<Box<DynPropertyValue>, ()> {
        Err(())
    }

    fn to_modbus(&self, _: &DynPropertyValue) -> Result<Vec<(Register, u16)>, ()> {
        Err(())
    }

    fn adjust_description(&self, description: &mut HomiePropertyDescription) {
        read_only(description)
    }
}

/// A heat curve: the supply line temperature the heat pump aims for at different outdoor
/// temperatures.
///
/// The outdoor temperatures (the X coordinates) are shared by all of the heat curves, the supply
/// line temperatures (the Y coordinates) are specific to the curve.
pub(crate) struct HeatCurveKind {
    registers: [Register; 14],
}

impl HeatCurveKind {
    /// The curve with the first Y coordinate in the specified holding register. The remaining six
    /// are in the registers that follow.
    pub(crate) const fn new(first_y: u16) -> HeatCurveKind {
        let mut registers = [Register::input(20); 14];
        let mut i = 0;
        while i < 7 {
            registers[i] = Register::input(20 + i as u16);
            registers[i + 7] = Register::holding(first_y + i as u16);
            i += 1;
        }
        HeatCurveKind { registers }
    }
}

impl PropertyKind for HeatCurveKind {
    fn registers(&self) -> &[Register] {
        &self.registers
    }

    fn value_from_modbus(&self, snapshot: &Snapshot) -> Option<Result<Box<DynPropertyValue>, ()>> {
        let mut points = Vec::new();
        for i in 0..7 {
            let outdoor = snapshot.value(self.registers[i])?.as_f64();
            let supply = snapshot.value(self.registers[i + 7])?.as_f64();
            points.push([outdoor, supply]);
        }
        Some(Ok(Box::new(HeatCurve(points))))
    }

    fn value_from_homie(&self, _: &str) -> Result<Box<DynPropertyValue>, ()> {
        Err(())
    }

    fn to_modbus(&self, _: &DynPropertyValue) -> Result<Vec<(Register, u16)>, ()> {
        Err(())
    }

    fn adjust_description(&self, description: &mut HomiePropertyDescription) {
        read_only(description)
    }
}

/// The units that a bit field with a bit for each of the (secondary) units has set, as a JSON
/// array of the numbers of the bits. The first unit is the bit 0.
pub(crate) struct UnitBits(u16);
impl UnitBits {
    pub(crate) fn new(bits: Value) -> Result<Self, ()> {
        Ok(Self(bits.raw()))
    }
}
impl PropertyValue for UnitBits {
    fn value(&self) -> String {
        json(&(0..u16::BITS).filter(|bit| self.0 & (1 << bit) != 0).collect::<Vec<_>>())
    }
}
impl PropertyDescription for UnitBits {
    fn description(_: &PropertyEntry) -> HomiePropertyDescription {
        json_description(r#"{"type":"array","items":{"type":"integer","minimum":0,"maximum":15}}"#)
    }
}
impl AggregatePropertyValue for UnitBits {
    const SETTABLE: bool = false;
}
impl TryFrom<&str> for UnitBits {
    type Error = ();
    fn try_from(_: &str) -> Result<Self, Self::Error> {
        Err(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alarms_are_found_and_named() {
        let alarms = alarms();
        assert!(alarms.registers.len() > 50);
        assert_eq!(alarms.registers.len(), alarms.names.len());
        assert!(alarms.names.iter().any(|n| n == "High pressure switch alarm"));
        // The class flags and the sum alarm are separate properties.
        assert!(!alarms.registers.contains(&Register::discrete_input(66)));
        assert!(!alarms.registers.contains(&Register::discrete_input(0)));
        let mut unique = alarms.names.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), alarms.names.len(), "alarm names are not unique");
    }

    #[test]
    fn demands() {
        let value = |bits| {
            let v = Value::new(Register::input(2), bits);
            RunningDemands::new(v).ok().unwrap().value()
        };
        assert_eq!(value(0), "[]");
        assert_eq!(value(0b1100), r#"["hot-water","heat"]"#);
        assert_eq!(value(1 << 10), r#"["no-demand"]"#);
    }

    #[test]
    fn heat_curve_registers() {
        let curve = HeatCurveKind::new(109);
        let registers = curve.registers();
        assert_eq!(registers[0], Register::input(20));
        assert_eq!(registers[6], Register::input(26));
        assert_eq!(registers[7], Register::holding(109));
        assert_eq!(registers[13], Register::holding(115));
    }

    #[test]
    fn unit_bits() {
        let bits = UnitBits::new(Value::new(Register::input(153), 0b101)).ok().unwrap();
        assert_eq!(bits.value(), "[0,2]");
    }

    #[test]
    fn schemas_are_json() {
        for schema in [STRING_LIST_SCHEMA, HEAT_CURVE_SCHEMA] {
            serde_json::from_str::<serde_json::Value>(schema).expect("valid JSON");
        }
    }
}
