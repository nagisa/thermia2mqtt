use crate::homie::value::PropertyValue;
use crate::modbus::Snapshot;
use crate::registers::{Register, Value};
use homie5::HomieID;
use homie5::device_description::{
    HomieNodeDescription, HomiePropertyDescription, PropertyDescriptionBuilder,
};
use std::collections::BTreeMap;
use std::marker::PhantomData;

/// A homie node: a group of related properties.
pub(crate) struct Node {
    pub id: HomieID,
    pub name: &'static str,
    pub properties: &'static [PropertyEntry],
}

impl Node {
    /// The description of the node with the properties that `include` says yes to.
    pub fn description(&self, include: impl Fn(usize) -> bool) -> HomieNodeDescription {
        let properties = self
            .properties
            .iter()
            .enumerate()
            .filter(|(i, _)| include(*i))
            .map(|(_, prop)| (prop.prop_id.clone(), prop.kind.description()))
            .collect::<BTreeMap<_, _>>();
        HomieNodeDescription { name: Some(self.name.to_string()), r#type: None, properties }
    }
}

/// How the value of a property is computed from the registers (and the other way around.)
pub(crate) trait PropertyKind: Send + Sync {
    /// The registers the value of the property depends on.
    fn registers(&self) -> &[Register];

    /// The value of the property, as it is to be published, computed from the registers.
    ///
    /// Returns `None` if some of the registers have not been read yet, and `Some(Err(()))` if what
    /// they hold is not a usable value.
    fn read(&self, snapshot: &Snapshot) -> Option<Result<String, ()>>;

    /// What needs to be written into the registers to set the property to the value received from
    /// MQTT. Fails if the property cannot be set to that value.
    fn write(&self, value: &str) -> Result<Vec<(Register, u16)>, ()>;

    fn description(&self) -> HomiePropertyDescription;
}

/// The unit as understood by homie.
fn homie_unit(unit: &str) -> Option<&'static str> {
    Some(match unit {
        "°C" => homie5::HOMIE_UNIT_DEGREE_CELSIUS,
        "%" => homie5::HOMIE_UNIT_PERCENT,
        "K" => "K",
        "A" => "A",
        "V" => "V",
        "W" => "W",
        "kWh" => "kWh",
        "kW" => "kW",
        "h" => "h",
        "min" => "min",
        "bar(g)" => "bar",
        _ => return None,
    })
}

/// A property that is a single register.
pub(crate) struct RegisterPropertyKind<T> {
    register: Register,
    _value: PhantomData<fn() -> T>,
}

impl<T> RegisterPropertyKind<T> {
    pub(crate) const fn new(register: Register) -> Self {
        Self { register, _value: PhantomData }
    }
}

impl<T> PropertyKind for RegisterPropertyKind<T>
where
    T: PropertyValue + TryFrom<Value> + for<'a> TryFrom<&'a str>,
{
    fn registers(&self) -> &[Register] {
        std::slice::from_ref(&self.register)
    }

    fn read(&self, snapshot: &Snapshot) -> Option<Result<String, ()>> {
        let value = snapshot.value(self.register)?;
        Some(T::try_from(value).map(|v| v.value()).map_err(|_| ()))
    }

    fn write(&self, value: &str) -> Result<Vec<(Register, u16)>, ()> {
        let value = T::try_from(value).map_err(|_| ())?;
        let raw = value.to_raw(self.register).ok_or(())?;
        Ok(vec![(self.register, raw)])
    }

    fn description(&self) -> HomiePropertyDescription {
        let mut description = T::description();
        description.settable = self.register.space().is_writable();
        if description.unit.is_none() {
            description.unit = homie_unit(self.register.def().unit).map(str::to_string);
        }
        description
    }
}

/// A property that is computed out of a number of registers. It is read-only.
pub(crate) struct AggregatePropertyKind {
    pub(crate) registers: &'static [Register],
    pub(crate) compute: fn(&Snapshot) -> Option<Result<String, ()>>,
    pub(crate) describe: fn() -> HomiePropertyDescription,
}

impl PropertyKind for AggregatePropertyKind {
    fn registers(&self) -> &[Register] {
        self.registers
    }

    fn read(&self, snapshot: &Snapshot) -> Option<Result<String, ()>> {
        (self.compute)(snapshot)
    }

    fn write(&self, _: &str) -> Result<Vec<(Register, u16)>, ()> {
        Err(())
    }

    fn description(&self) -> HomiePropertyDescription {
        (self.describe)()
    }
}

/// A property that, when set to `now`, makes something happen. It has no value.
pub(crate) struct ActionPropertyKind {
    pub(crate) writes: &'static [(Register, u16)],
}

impl PropertyKind for ActionPropertyKind {
    fn registers(&self) -> &[Register] {
        &[]
    }

    fn read(&self, _: &Snapshot) -> Option<Result<String, ()>> {
        None
    }

    fn write(&self, value: &str) -> Result<Vec<(Register, u16)>, ()> {
        (value == "now").then(|| self.writes.to_vec()).ok_or(())
    }

    fn description(&self) -> HomiePropertyDescription {
        let mut description = PropertyDescriptionBuilder::enumeration(["now"])
            .expect("enum values are valid")
            .build();
        description.retained = false;
        description.settable = true;
        description
    }
}

pub(crate) struct PropertyEntry {
    pub prop_id: HomieID,
    pub kind: &'static dyn PropertyKind,
}

/// Declare the properties of a node.
///
/// Each property is `{ "id": kind }`, where `kind` is one of:
///
/// * `ValueType = register space(address)` -- a property that is a single register;
/// * `aggregate(describe) = compute(space(address), …)` -- the value is `compute` of these
///   registers, described as `describe` in [`crate::homie::value`];
/// * `action = &[(register, word), …]` -- a property that writes these when set to `now`;
/// * `custom expression` -- a hand-written [`PropertyKind`].
macro_rules! properties {
    (static $static:ident = [$( { $prop_id:literal: $($kind:tt)* }, )*]) => {
        static $static: [PropertyEntry; 0 $(+ $crate::homie::node::properties!(@one $prop_id))*] = [
            $($crate::homie::node::properties!(@property $prop_id: $($kind)*)),*
        ];
    };

    (@one $prop_id: literal) => { 1 };

    (@property $prop_id:literal: action = $writes:expr) => {
        PropertyEntry {
            prop_id: HomieID::new_const($prop_id),
            kind: &$crate::homie::node::ActionPropertyKind { writes: $writes },
        }
    };
    (@property $prop_id:literal: aggregate($describe:ident) = $compute:ident(
        $($space:ident($address:literal)),+
    )) => {
        PropertyEntry {
            prop_id: HomieID::new_const($prop_id),
            kind: &$crate::homie::node::AggregatePropertyKind {
                registers: &[$($crate::registers::Register::$space($address)),+],
                compute: |snapshot| Some($compute($(
                    snapshot.value(const { $crate::registers::Register::$space($address) })?
                ),+)),
                describe: $crate::homie::value::$describe,
            },
        }
    };
    (@property $prop_id:literal: custom $kind:expr) => {
        PropertyEntry { prop_id: HomieID::new_const($prop_id), kind: &$kind }
    };
    (@property $prop_id:literal: $value_type:ty = register $space:ident ($address:literal)) => {
        PropertyEntry {
            prop_id: HomieID::new_const($prop_id),
            kind: &$crate::homie::node::RegisterPropertyKind::<$value_type>::new(
                $crate::registers::Register::$space($address),
            ),
        }
    };
}

pub(crate) use properties;
