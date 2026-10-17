use crate::homie::value::{
    ActionPropertyValue, AggregatePropertyValue, DynPropertyValue, PropertyValue,
    RegisterPropertyValue,
};
use crate::modbus::Snapshot;
use crate::registers::{Register, Value};
use homie5::HomieID;
use homie5::device_description::{HomieNodeDescription, HomiePropertyDescription};
use std::any::Any;
use std::collections::BTreeMap;

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
            .map(|(_, prop)| (prop.prop_id.clone(), prop.description()))
            .collect::<BTreeMap<_, _>>();
        HomieNodeDescription { name: Some(self.name.to_string()), r#type: None, properties }
    }
}

/// How the value of a property is computed from the registers (and the other way around.)
pub(crate) trait PropertyKind: Send + Sync {
    /// The registers the value of the property depends on.
    fn registers(&self) -> &[Register];

    /// Compute the value of the property from the registers.
    ///
    /// Returns `None` if we do not have all the registers yet.
    fn value_from_modbus(&self, snapshot: &Snapshot) -> Option<Result<Box<DynPropertyValue>, ()>>;

    /// Parse the value coming from the MQTT side.
    fn value_from_homie(&self, mqtt: &str) -> Result<Box<DynPropertyValue>, ()>;

    /// What needs to be written into the registers to set the property to the specified value.
    fn to_modbus(&self, value: &DynPropertyValue) -> Result<Vec<(Register, u16)>, ()>;

    fn adjust_description(&self, description: &mut HomiePropertyDescription);
}

fn downcast<T: PropertyValue>(value: &DynPropertyValue) -> &T {
    (value as &dyn Any).downcast_ref::<T>().expect("type confusion")
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
        "h" => "h",
        "min" => "min",
        "bar(g)" => "bar",
        _ => return None,
    })
}

/// A property that is a single register.
pub(crate) struct RegisterPropertyKind<T> {
    pub register: Register,
    pub _phantom: std::marker::PhantomData<fn() -> T>,
}

impl<T> PropertyKind for RegisterPropertyKind<T>
where
    T: PropertyValue + RegisterPropertyValue + 'static,
    T: TryFrom<Value> + for<'a> TryFrom<&'a str>,
{
    fn registers(&self) -> &[Register] {
        std::slice::from_ref(&self.register)
    }

    fn value_from_modbus(&self, snapshot: &Snapshot) -> Option<Result<Box<DynPropertyValue>, ()>> {
        let value = snapshot.value(self.register)?;
        Some(T::try_from(value).map(|v| Box::new(v) as _).map_err(|_| ()))
    }

    fn value_from_homie(&self, mqtt: &str) -> Result<Box<DynPropertyValue>, ()> {
        T::try_from(mqtt).map(|v| Box::new(v) as _).map_err(|_| ())
    }

    fn to_modbus(&self, value: &DynPropertyValue) -> Result<Vec<(Register, u16)>, ()> {
        let raw = downcast::<T>(value).to_raw(self.register).ok_or(())?;
        Ok(vec![(self.register, raw)])
    }

    fn adjust_description(&self, description: &mut HomiePropertyDescription) {
        description.settable = self.register.space().is_writable();
        description.retained = true;
        if description.unit.is_none() {
            description.unit = homie_unit(self.register.def().unit).map(str::to_string);
        }
    }
}

/// The function that computes the value of an [`AggregatePropertyKind`].
pub(crate) type AggregateFromModbus = fn(&Snapshot) -> Option<Result<Box<DynPropertyValue>, ()>>;

/// A property that is computed out of a number of registers.
pub(crate) struct AggregatePropertyKind<T> {
    pub registers: &'static [Register],
    // Macro-generated constructor that delegates to `::new()`.
    pub from_modbus: AggregateFromModbus,
    pub _phantom: std::marker::PhantomData<fn() -> T>,
}

impl<T> PropertyKind for AggregatePropertyKind<T>
where
    T: PropertyValue + AggregatePropertyValue + 'static,
    T: for<'a> TryFrom<&'a str>,
{
    fn registers(&self) -> &[Register] {
        self.registers
    }

    fn value_from_modbus(&self, snapshot: &Snapshot) -> Option<Result<Box<DynPropertyValue>, ()>> {
        (self.from_modbus)(snapshot)
    }

    fn value_from_homie(&self, mqtt: &str) -> Result<Box<DynPropertyValue>, ()> {
        T::try_from(mqtt).map(|v| Box::new(v) as _).map_err(|_| ())
    }

    fn to_modbus(&self, value: &DynPropertyValue) -> Result<Vec<(Register, u16)>, ()> {
        if !T::SETTABLE {
            return Err(());
        }
        Ok(downcast::<T>(value).writes())
    }

    fn adjust_description(&self, description: &mut HomiePropertyDescription) {
        description.retained = true;
        description.settable = T::SETTABLE;
    }
}

/// A property that, when set, makes something happen.
pub(crate) struct ActionPropertyKind<T> {
    pub _phantom: std::marker::PhantomData<fn() -> T>,
}

impl<T> PropertyKind for ActionPropertyKind<T>
where
    T: PropertyValue + ActionPropertyValue + 'static,
    T: for<'a> TryFrom<&'a str>,
{
    fn registers(&self) -> &[Register] {
        &[]
    }

    fn value_from_modbus(&self, _: &Snapshot) -> Option<Result<Box<DynPropertyValue>, ()>> {
        None
    }

    fn value_from_homie(&self, mqtt: &str) -> Result<Box<DynPropertyValue>, ()> {
        T::try_from(mqtt).map(|v| Box::new(v) as _).map_err(|_| ())
    }

    fn to_modbus(&self, value: &DynPropertyValue) -> Result<Vec<(Register, u16)>, ()> {
        Ok(downcast::<T>(value).writes())
    }

    fn adjust_description(&self, description: &mut HomiePropertyDescription) {
        description.retained = false;
        description.settable = true;
    }
}

pub(crate) struct PropertyEntry {
    pub prop_id: HomieID,
    pub mk_description: fn(&PropertyEntry) -> HomiePropertyDescription,
    pub kind: &'static dyn PropertyKind,
}

impl PropertyEntry {
    pub fn description(&self) -> HomiePropertyDescription {
        let mut initial = (self.mk_description)(self);
        self.kind.adjust_description(&mut initial);
        initial
    }
}

/// Declare the properties of a node.
///
/// Each property is `{ "id": ValueType = kind }`, where `kind` is one of:
///
/// * `register space(address)` -- a property that is a single register;
/// * `aggregate space(address), space(address), …` -- the value is computed by
///   `ValueType::new(value, value, …)` out of these registers;
/// * `action` -- a property that does something when set;
/// * `custom expression` -- a hand-written [`PropertyKind`].
macro_rules! properties {
    (static $static:ident = [$( { $prop_id:literal: $value_type:ty = $($tokens:tt)* }, )*]) => {
        static $static: [PropertyEntry; 0 $(+ $crate::homie::node::properties!(@one $prop_id))*] = [
            $($crate::homie::node::properties!(@property $prop_id: $value_type = $($tokens)*)),*
        ];
    };

    (@one $prop_id: literal) => { 1 };

    (@property $prop_id:literal: $value_type:ty = register $space:ident ($address:literal)) => {
        PropertyEntry {
            prop_id: HomieID::new_const($prop_id),
            mk_description: <$value_type as $crate::homie::value::PropertyDescription>::description,
            kind: &$crate::homie::node::RegisterPropertyKind::<$value_type> {
                register: $crate::registers::Register::$space($address),
                _phantom: std::marker::PhantomData
            }
        }
    };
    (@property $prop_id:literal: $value_type:ty = action) => {
        PropertyEntry {
            prop_id: HomieID::new_const($prop_id),
            mk_description: <$value_type as $crate::homie::value::PropertyDescription>::description,
            kind: &$crate::homie::node::ActionPropertyKind::<$value_type> {
                _phantom: std::marker::PhantomData
            },
        }
    };
    (@property $prop_id:literal: $value_type:ty = aggregate $($space:ident ($address:literal)),+) => {
        PropertyEntry {
            prop_id: HomieID::new_const($prop_id),
            mk_description: <$value_type as $crate::homie::value::PropertyDescription>::description,
            kind: &$crate::homie::node::AggregatePropertyKind::<$value_type> {
                registers: &[$($crate::registers::Register::$space($address)),*],
                from_modbus: |snapshot| {
                    let result = <$value_type>::new($(
                        snapshot.value(const { $crate::registers::Register::$space($address) })?
                    ),*);
                    Some(result.map(|v| Box::new(v) as _))
                },
                _phantom: std::marker::PhantomData,
            }
        }
    };
    (@property $prop_id:literal: $value_type:ty = custom $kind:expr) => {
        PropertyEntry {
            prop_id: HomieID::new_const($prop_id),
            mk_description: <$value_type as $crate::homie::value::PropertyDescription>::description,
            kind: &$kind,
        }
    };
}

pub(crate) use properties;
