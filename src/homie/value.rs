//! The values of the homie properties and how they relate to the modbus registers.

use crate::homie::node::PropertyEntry;
use crate::registers::{self, Register, Value};
use homie5::device_description::{HomiePropertyDescription, PropertyDescriptionBuilder};
use std::any::Any;

pub(crate) trait PropertyValue: Any + Send + Sync {
    /// The value as it is to be published.
    fn value(&self) -> String;
}

pub(crate) type DynPropertyValue = dyn Send + Sync + PropertyValue;

pub(crate) trait PropertyDescription {
    fn description(prop: &PropertyEntry) -> HomiePropertyDescription;
}

/// [`PropertyValue`] types that directly correspond to a single modbus register.
pub(crate) trait RegisterPropertyValue {
    /// The word to write into the register to set the property to this value.
    ///
    /// Returns `None` if the value cannot be represented.
    fn to_raw(&self, register: Register) -> Option<u16>;
}

/// [`PropertyValue`] types that are computed out of multiple registers.
pub(crate) trait AggregatePropertyValue {
    const SETTABLE: bool;
    /// What to write to the registers to set the property to this value.
    fn writes(&self) -> Vec<(Register, u16)> {
        unreachable!("aggregate is not settable")
    }
}

/// [`PropertyValue`] types that trigger something when set.
pub(crate) trait ActionPropertyValue {
    fn writes(&self) -> Vec<(Register, u16)>;
}

pub(crate) fn homie_enum<T: strum::VariantNames>() -> HomiePropertyDescription {
    let names = <T as strum::VariantNames>::VARIANTS.iter().copied();
    PropertyDescriptionBuilder::enumeration(names).expect("enum values are valid").build()
}

pub(crate) struct BooleanValue(pub(crate) bool);
impl PropertyDescription for BooleanValue {
    fn description(_prop: &PropertyEntry) -> HomiePropertyDescription {
        PropertyDescriptionBuilder::boolean().build()
    }
}
impl TryFrom<Value> for BooleanValue {
    type Error = ();
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        Ok(BooleanValue(value.as_bool()))
    }
}
impl TryFrom<&str> for BooleanValue {
    type Error = ();
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self(value.parse().map_err(|_| ())?))
    }
}
impl PropertyValue for BooleanValue {
    fn value(&self) -> String {
        self.0.to_string()
    }
}
impl RegisterPropertyValue for BooleanValue {
    fn to_raw(&self, _: Register) -> Option<u16> {
        Some(u16::from(self.0))
    }
}

pub(crate) mod unit {
    /// Units that the guide does not specify, but are known.
    pub(crate) trait Unit {
        const UNIT: Option<&'static str>;
    }
    pub(crate) struct None;
    impl Unit for None {
        const UNIT: Option<&'static str> = Option::None;
    }
    pub(crate) struct Rpm;
    impl Unit for Rpm {
        const UNIT: Option<&'static str> = Some("rpm");
    }
}

/// A whole number, as opposed to a [`FloatValue`].
///
/// The unit is the one specified by the register unless `U` says otherwise.
pub(crate) struct IntegerValue<U = unit::None>(pub(crate) i32, std::marker::PhantomData<fn() -> U>);
impl<U: unit::Unit> PropertyDescription for IntegerValue<U> {
    fn description(_prop: &PropertyEntry) -> HomiePropertyDescription {
        let mut description = PropertyDescriptionBuilder::integer().build();
        description.unit = U::UNIT.map(str::to_string);
        description
    }
}
impl<U> TryFrom<Value> for IntegerValue<U> {
    type Error = ();
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if value.register().def().scale != 1 {
            return Err(());
        }
        Ok(Self(value.as_i32(), std::marker::PhantomData))
    }
}
impl<U> TryFrom<&str> for IntegerValue<U> {
    type Error = ();
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Self(value.parse().map_err(|_| ())?, std::marker::PhantomData))
    }
}
impl<U: unit::Unit + 'static> PropertyValue for IntegerValue<U> {
    fn value(&self) -> String {
        self.0.to_string()
    }
}
impl<U> RegisterPropertyValue for IntegerValue<U> {
    fn to_raw(&self, register: Register) -> Option<u16> {
        registers::to_raw(register, f64::from(self.0))
    }
}

/// A number with a fractional part. The unit comes from the register.
pub(crate) struct FloatValue(pub(crate) f64);
impl PropertyDescription for FloatValue {
    fn description(_prop: &PropertyEntry) -> HomiePropertyDescription {
        PropertyDescriptionBuilder::float().build()
    }
}
/// The temperature the heat pump reports for the sensors that are not there (or have not been
/// given a value.) It is also the highest valid temperature, see the guide's description of the
/// BMS outdoor temperature register.
const NO_TEMPERATURE: f64 = 200.0;

impl TryFrom<Value> for FloatValue {
    type Error = ();
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let number = value.as_f64();
        if value.register().def().unit == "°C" && number >= NO_TEMPERATURE {
            return Err(());
        }
        Ok(Self(number))
    }
}
impl TryFrom<&str> for FloatValue {
    type Error = ();
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let value = value.parse::<f64>().map_err(|_| ())?;
        value.is_finite().then_some(Self(value)).ok_or(())
    }
}
impl PropertyValue for FloatValue {
    fn value(&self) -> String {
        self.0.to_string()
    }
}
impl RegisterPropertyValue for FloatValue {
    fn to_raw(&self, register: Register) -> Option<u16> {
        registers::to_raw(register, self.0)
    }
}

/// A version number out of three registers.
pub(crate) struct VersionValue(u16, u16, u16);
impl VersionValue {
    pub(crate) fn new(major: Value, minor: Value, micro: Value) -> Result<Self, ()> {
        Ok(Self(major.raw(), minor.raw(), micro.raw()))
    }
}
impl PropertyValue for VersionValue {
    fn value(&self) -> String {
        format!("{}.{}.{}", self.0, self.1, self.2)
    }
}
impl PropertyDescription for VersionValue {
    fn description(_: &PropertyEntry) -> HomiePropertyDescription {
        PropertyDescriptionBuilder::string().build()
    }
}
impl AggregatePropertyValue for VersionValue {
    const SETTABLE: bool = false;
}
impl TryFrom<&str> for VersionValue {
    type Error = ();
    fn try_from(_: &str) -> Result<Self, Self::Error> {
        Err(())
    }
}

/// For how long something has been operating. The counter does not fit into 16 bits, so it is split
/// over the most and the least significant words.
pub(crate) struct OperatingTime(jiff::Span);
impl OperatingTime {
    pub(crate) fn new(most_significant: Value, least_significant: Value) -> Result<Self, ()> {
        let hours = u32::from(most_significant.raw()) << 16 | u32::from(least_significant.raw());
        jiff::Span::new().try_hours(i64::from(hours)).map(Self).map_err(|_| ())
    }
}
impl PropertyValue for OperatingTime {
    fn value(&self) -> String {
        self.0.to_string()
    }
}
impl PropertyDescription for OperatingTime {
    fn description(_: &PropertyEntry) -> HomiePropertyDescription {
        PropertyDescriptionBuilder::duration().build()
    }
}
impl AggregatePropertyValue for OperatingTime {
    const SETTABLE: bool = false;
}
impl TryFrom<&str> for OperatingTime {
    type Error = ();
    fn try_from(_: &str) -> Result<Self, Self::Error> {
        Err(())
    }
}

/// The 32-bit energy counter of the electric meter. The counter is scaled by 10.
pub(crate) struct EnergyValue(f64);
impl EnergyValue {
    pub(crate) fn new(least_significant: Value, most_significant: Value) -> Result<Self, ()> {
        let scale = f64::from(least_significant.register().def().scale);
        let words = u32::from(most_significant.raw()) << 16 | u32::from(least_significant.raw());
        Ok(Self(f64::from(words) / scale))
    }
}
impl PropertyValue for EnergyValue {
    fn value(&self) -> String {
        self.0.to_string()
    }
}
impl PropertyDescription for EnergyValue {
    fn description(_: &PropertyEntry) -> HomiePropertyDescription {
        PropertyDescriptionBuilder::float().unit("kWh").build()
    }
}
impl AggregatePropertyValue for EnergyValue {
    const SETTABLE: bool = false;
}
impl TryFrom<&str> for EnergyValue {
    type Error = ();
    fn try_from(_: &str) -> Result<Self, Self::Error> {
        Err(())
    }
}

macro_rules! string_enum {
    (
        #[impl($($impl:ident),*)]
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $($variant:ident = $value:literal),* $(,)?
        }
    ) => {
        #[derive(
            strum::VariantNames,
            strum::FromRepr,
            strum::IntoStaticStr,
            strum::EnumString,
        )]
        #[strum(serialize_all = "kebab-case")]
        $(#[$meta])*
        $vis enum $name {
            $($variant = $value),*
        }

        $($crate::homie::value::string_enum!(@impl $impl for $name);)*
    };
    (@impl TryFromValue for $name:ident) => {
        impl TryFrom<$crate::registers::Value> for $name {
            type Error = ();
            fn try_from(value: $crate::registers::Value) -> Result<Self, Self::Error> {
                Self::from_repr(value.raw()).ok_or(())
            }
        }
    };
    (@impl PropertyValue for $name:ident) => {
        impl $crate::homie::value::PropertyValue for $name {
            fn value(&self) -> String {
                <&'static str>::from(self).to_string()
            }
        }
    };
    (@impl RegisterPropertyValue for $name:ident) => {
        impl $crate::homie::value::RegisterPropertyValue for $name {
            fn to_raw(&self, _: $crate::registers::Register) -> Option<u16> {
                Some(*self as u16)
            }
        }
    };
    (@impl PropertyDescription for $name:ident) => {
        impl $crate::homie::value::PropertyDescription for $name {
            fn description(_: &$crate::homie::node::PropertyEntry)
            -> homie5::device_description::HomiePropertyDescription {
                $crate::homie::value::homie_enum::<Self>()
            }
        }
    };
}

pub(crate) use string_enum;

/// A duration that the register holds as a whole number of seconds.
pub(crate) struct SecondsValue(jiff::Span);
impl PropertyDescription for SecondsValue {
    fn description(_prop: &PropertyEntry) -> HomiePropertyDescription {
        PropertyDescriptionBuilder::duration().build()
    }
}
impl TryFrom<Value> for SecondsValue {
    type Error = ();
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if value.register().def().scale != 1 {
            return Err(());
        }
        jiff::Span::new().try_seconds(value.as_i32()).map(Self).map_err(|_| ())
    }
}
impl TryFrom<&str> for SecondsValue {
    type Error = ();
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let span = value.parse::<jiff::Span>().map_err(|_| ())?;
        let seconds = span.total(jiff::Unit::Second).map_err(|_| ())?;
        jiff::Span::new().try_seconds(seconds.round() as i64).map(Self).map_err(|_| ())
    }
}
impl PropertyValue for SecondsValue {
    fn value(&self) -> String {
        self.0.to_string()
    }
}
impl RegisterPropertyValue for SecondsValue {
    fn to_raw(&self, register: Register) -> Option<u16> {
        let seconds = self.0.total(jiff::Unit::Second).ok()?;
        registers::to_raw(register, seconds)
    }
}

/// A duration that the register holds as a whole number of minutes.
pub(crate) struct MinutesValue(jiff::Span);
impl PropertyDescription for MinutesValue {
    fn description(_prop: &PropertyEntry) -> HomiePropertyDescription {
        PropertyDescriptionBuilder::duration().build()
    }
}
impl TryFrom<Value> for MinutesValue {
    type Error = ();
    fn try_from(value: Value) -> Result<Self, Self::Error> {
        if value.register().def().scale != 1 {
            return Err(());
        }
        jiff::Span::new().try_minutes(value.as_i32()).map(Self).map_err(|_| ())
    }
}
impl TryFrom<&str> for MinutesValue {
    type Error = ();
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let span = value.parse::<jiff::Span>().map_err(|_| ())?;
        let minutes = span.total(jiff::Unit::Minute).map_err(|_| ())?;
        jiff::Span::new().try_minutes(minutes.round() as i64).map(Self).map_err(|_| ())
    }
}
impl PropertyValue for MinutesValue {
    fn value(&self) -> String {
        self.0.to_string()
    }
}
impl RegisterPropertyValue for MinutesValue {
    fn to_raw(&self, register: Register) -> Option<u16> {
        let minutes = self.0.total(jiff::Unit::Minute).ok()?;
        registers::to_raw(register, minutes)
    }
}
