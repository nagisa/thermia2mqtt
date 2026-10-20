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

    /// The value the property is being brought to, as it is to be published in `$target`.
    ///
    /// Same as [`read`](Self::read), but `None` also means that the property has no target.
    fn target(&self, _snapshot: &Snapshot) -> Option<Result<String, ()>> {
        None
    }

    /// Whether the property can be set, given what the heat pump has. The description says
    /// `settable` only if this is so.
    fn is_settable(&self, _snapshot: &Snapshot) -> bool {
        true
    }

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

/// A property whose value is one register, set by writing another: the value is what is in effect,
/// and the other register is the target that the value is to reach.
///
/// The target is published as the `$target` attribute of the property. Heat pumps that do not have
/// the register for the target have the property read-only and without the target.
pub(crate) struct TargetedPropertyKind<V, T> {
    /// The register of the value and the register of the target.
    registers: [Register; 2],
    _types: PhantomData<fn() -> (V, T)>,
}

impl<V, T> TargetedPropertyKind<V, T> {
    pub(crate) const fn new(value: Register, target: Register) -> Self {
        Self { registers: [value, target], _types: PhantomData }
    }

    fn value_register(&self) -> Register {
        self.registers[0]
    }

    fn target_register(&self) -> Register {
        self.registers[1]
    }
}

impl<V, T> PropertyKind for TargetedPropertyKind<V, T>
where
    V: PropertyValue + TryFrom<Value>,
    T: PropertyValue + TryFrom<Value> + for<'a> TryFrom<&'a str>,
{
    fn registers(&self) -> &[Register] {
        &self.registers
    }

    fn read(&self, snapshot: &Snapshot) -> Option<Result<String, ()>> {
        let value = snapshot.value(self.value_register())?;
        Some(V::try_from(value).map(|v| v.value()).map_err(|_| ()))
    }

    fn target(&self, snapshot: &Snapshot) -> Option<Result<String, ()>> {
        let value = snapshot.value(self.target_register())?;
        Some(T::try_from(value).map(|v| v.value()).map_err(|_| ()))
    }

    fn is_settable(&self, snapshot: &Snapshot) -> bool {
        !snapshot.is_unreadable(self.target_register())
    }

    fn write(&self, value: &str) -> Result<Vec<(Register, u16)>, ()> {
        let value = T::try_from(value).map_err(|_| ())?;
        let raw = value.to_raw(self.target_register()).ok_or(())?;
        Ok(vec![(self.target_register(), raw)])
    }

    fn description(&self) -> HomiePropertyDescription {
        let mut description = V::description();
        description.settable = self.target_register().space().is_writable();
        if description.unit.is_none() {
            description.unit = homie_unit(self.value_register().def().unit).map(str::to_string);
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
/// * `ValueType = register space(address), target TargetType = space(address)` -- a property
///   that is a single register, and is set by writing to another one (which is also the `$target`
///   of the property);
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
    (@property $prop_id:literal: $value_type:ty = register $space:ident ($address:literal),
        target $target_type:ty = $target_space:ident ($target_address:literal)
    ) => {
        PropertyEntry {
            prop_id: HomieID::new_const($prop_id),
            kind: &$crate::homie::node::TargetedPropertyKind::<$value_type, $target_type>::new(
                $crate::registers::Register::$space($address),
                $crate::registers::Register::$target_space($target_address),
            ),
        }
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
