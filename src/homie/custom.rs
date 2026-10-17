//! Properties that do not fit the mold of the generic kinds.

use crate::homie::node::PropertyKind;
use crate::homie::value::{json, json_description, string_list};
use crate::modbus::Snapshot;
use crate::registers::{Register, Space, Value};
use homie5::device_description::HomiePropertyDescription;
use std::sync::OnceLock;

const HEAT_CURVE_SCHEMA: &str = r#"{"type":"array","items":{"type":"array","items":{"type":"number"},"minItems":2,"maxItems":2}}"#;

/// Which kinds of operation the heat pump is carrying out at the moment, as a JSON array.
///
/// This is a bit field, see footnote 6 in the guide.
pub(crate) fn running_demands(bits: Value) -> Result<String, ()> {
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
    let active = DEMAND_BITS.iter().filter(|(bit, _)| bits.raw() & (1 << bit) != 0);
    Ok(json(&active.map(|(_, name)| *name).collect::<Vec<_>>()))
}

/// The units that a bit field with a bit for each of the (secondary) units has set, as a JSON
/// array of the numbers of the bits. The first unit is the bit 0.
pub(crate) fn unit_bits(bits: Value) -> Result<String, ()> {
    Ok(json(&(0..u16::BITS).filter(|bit| bits.raw() & (1 << bit) != 0).collect::<Vec<_>>()))
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

    fn read(&self, snapshot: &Snapshot) -> Option<Result<String, ()>> {
        let alarms = alarms();
        let mut active = Vec::new();
        for (register, name) in alarms.registers.iter().zip(&alarms.names) {
            match snapshot.value(*register) {
                Some(value) if value.as_bool() => active.push(name.as_str()),
                Some(_) => {}
                // An alarm that the heat pump does not have is never active.
                None if snapshot.is_unreadable(*register) => {}
                None => return None,
            }
        }
        Some(Ok(json(&active)))
    }

    fn write(&self, _: &str) -> Result<Vec<(Register, u16)>, ()> {
        Err(())
    }

    fn description(&self) -> HomiePropertyDescription {
        string_list()
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

    fn read(&self, snapshot: &Snapshot) -> Option<Result<String, ()>> {
        let mut points = Vec::new();
        for i in 0..7 {
            let outdoor = snapshot.value(self.registers[i])?.as_f64();
            let supply = snapshot.value(self.registers[i + 7])?.as_f64();
            points.push([outdoor, supply]);
        }
        Some(Ok(json(&points)))
    }

    fn write(&self, _: &str) -> Result<Vec<(Register, u16)>, ()> {
        Err(())
    }

    fn description(&self) -> HomiePropertyDescription {
        json_description(HEAT_CURVE_SCHEMA)
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
        let value = |bits| running_demands(Value::new(Register::input(2), bits)).unwrap();
        assert_eq!(value(0), "[]");
        assert_eq!(value(0b1100), r#"["hot-water","heat"]"#);
        assert_eq!(value(1 << 10), r#"["no-demand"]"#);
    }

    #[test]
    fn unit_bits_are_listed_by_number() {
        let bits = unit_bits(Value::new(Register::input(153), 0b101)).unwrap();
        assert_eq!(bits, "[0,2]");
    }
}
