//! The values of the homie properties and how they relate to the modbus registers.

use crate::registers::{self, Register, Value};
use homie5::device_description::{HomiePropertyDescription, PropertyDescriptionBuilder};

/// A value of a property that is a single register.
pub(crate) trait PropertyValue {
    /// How the values are described to the controllers.
    fn description() -> HomiePropertyDescription;
    /// The value as it is to be published.
    fn value(&self) -> String;
    /// The word to write into the register to set the property to this value.
    ///
    /// Returns `None` if the value cannot be represented.
    fn to_raw(&self, register: Register) -> Option<u16>;
}

const STRING_LIST_SCHEMA: &str = r#"{"type":"array","items":{"type":"string"}}"#;
const UNIT_LIST_SCHEMA: &str =
    r#"{"type":"array","items":{"type":"integer","minimum":0,"maximum":15}}"#;

pub(crate) fn json<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("serializing to JSON cannot fail")
}

pub(crate) fn json_description(schema: &str) -> HomiePropertyDescription {
    PropertyDescriptionBuilder::json().json_schema(schema).build()
}

pub(crate) fn homie_enum<T: strum::VariantNames>() -> HomiePropertyDescription {
    let names = <T as strum::VariantNames>::VARIANTS.iter().copied();
    PropertyDescriptionBuilder::enumeration(names).expect("enum values are valid").build()
}

// The descriptions of the aggregates, see `properties!` in `node.rs`.

pub(crate) fn text() -> HomiePropertyDescription {
    PropertyDescriptionBuilder::string().build()
}

pub(crate) fn duration() -> HomiePropertyDescription {
    PropertyDescriptionBuilder::duration().build()
}

pub(crate) fn float() -> HomiePropertyDescription {
    PropertyDescriptionBuilder::float().build()
}

pub(crate) fn kilowatt_hours() -> HomiePropertyDescription {
    PropertyDescriptionBuilder::float().unit("kWh").build()
}

pub(crate) fn string_list() -> HomiePropertyDescription {
    json_description(STRING_LIST_SCHEMA)
}

pub(crate) fn unit_list() -> HomiePropertyDescription {
    json_description(UNIT_LIST_SCHEMA)
}

// Aggregates: values computed out of several registers, in the order that the property lists them.
pub(crate) fn version(major: Value, minor: Value, micro: Value) -> Result<String, ()> {
    Ok(format!("{}.{}.{}", major.raw(), minor.raw(), micro.raw()))
}

/// For how long something has been operating. The counter does not fit into 16 bits, so it is split
/// over the most and the least significant words.
pub(crate) fn operating_time(most: Value, least: Value) -> Result<String, ()> {
    let hours = u32::from(most.raw()) << 16 | u32::from(least.raw());
    let span = jiff::Span::new().try_hours(i64::from(hours)).map_err(|_| ())?;
    Ok(span.to_string())
}

/// The 32-bit energy counter of the electric meter. The counter is scaled by 10.
pub(crate) fn meter_energy(least: Value, most: Value) -> Result<String, ()> {
    let scale = f64::from(least.register().def().scale);
    let words = u32::from(most.raw()) << 16 | u32::from(least.raw());
    Ok((f64::from(words) / scale).to_string())
}

/// Energy that the heat pump keeps in two registers: one has the kWh (up to 999), the other the
/// MWh (each being 1000 kWh.)
pub(crate) fn split_energy(kwh: Value, mwh: Value) -> Result<String, ()> {
    Ok((u32::from(mwh.raw()) * 1000 + u32::from(kwh.raw())).to_string())
}

/// The coefficient of performance right now: how much heat the heat pump delivers for the
/// electricity it consumes. It is 0 while nothing is consumed.
pub(crate) fn coefficient_of_performance(output: Value, consumed: Value) -> Result<String, ()> {
    if consumed.raw() == 0 {
        return Ok(0.0f64.to_string());
    }
    let ratio = f64::from(output.raw()) / f64::from(consumed.raw());
    Ok(((ratio * 100.0).round() / 100.0).to_string())
}

pub(crate) struct BooleanValue(bool);
impl PropertyValue for BooleanValue {
    fn description() -> HomiePropertyDescription {
        PropertyDescriptionBuilder::boolean().build()
    }
    fn value(&self) -> String {
        self.0.to_string()
    }
    fn to_raw(&self, _: Register) -> Option<u16> {
        Some(u16::from(self.0))
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
        value.parse().map(BooleanValue).map_err(|_| ())
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
pub(crate) struct IntegerValue<U = unit::None>(i32, std::marker::PhantomData<fn() -> U>);
impl<U: unit::Unit> PropertyValue for IntegerValue<U> {
    fn description() -> HomiePropertyDescription {
        let mut description = PropertyDescriptionBuilder::integer().build();
        description.unit = U::UNIT.map(str::to_string);
        description
    }
    fn value(&self) -> String {
        self.0.to_string()
    }
    fn to_raw(&self, register: Register) -> Option<u16> {
        registers::to_raw(register, f64::from(self.0))
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

/// A number with a fractional part. The unit comes from the register.
pub(crate) struct FloatValue(f64);

/// The temperature the heat pump reports for the sensors that are not there (or have not been
/// given a value.) It is also the highest valid temperature, see the guide's description of the
/// BMS outdoor temperature register.
const NO_TEMPERATURE: f64 = 200.0;

impl PropertyValue for FloatValue {
    fn description() -> HomiePropertyDescription {
        float()
    }
    fn value(&self) -> String {
        self.0.to_string()
    }
    fn to_raw(&self, register: Register) -> Option<u16> {
        registers::to_raw(register, self.0)
    }
}
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

/// Declare an enum of the values that a register holds. The register values that are not listed
/// are published as `unknown`, which can be read but not set.
macro_rules! string_enum {
    (
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
            $($variant = $value,)*
            /// The register holds a value that this program does not know the meaning of.
            ///
            /// This can be read, but not set.
            Unknown = 0xFFFF,
        }

        impl From<$crate::registers::Value> for $name {
            fn from(value: $crate::registers::Value) -> Self {
                Self::from_repr(value.raw()).unwrap_or_else(|| {
                    tracing::debug!(
                        register = ?value.register(),
                        raw = value.raw(),
                        "unrecognized value for {}",
                        stringify!($name),
                    );
                    Self::Unknown
                })
            }
        }

        impl $crate::homie::value::PropertyValue for $name {
            fn description() -> homie5::device_description::HomiePropertyDescription {
                $crate::homie::value::homie_enum::<Self>()
            }
            fn value(&self) -> String {
                <&'static str>::from(self).to_string()
            }
            fn to_raw(&self, _: $crate::registers::Register) -> Option<u16> {
                (!matches!(self, Self::Unknown)).then_some(*self as u16)
            }
        }
    };
}

pub(crate) use string_enum;

/// A duration that the register holds as a whole number of seconds.
pub(crate) struct SecondsValue(jiff::Span);
impl PropertyValue for SecondsValue {
    fn description() -> HomiePropertyDescription {
        duration()
    }
    fn value(&self) -> String {
        self.0.to_string()
    }
    fn to_raw(&self, register: Register) -> Option<u16> {
        let seconds = self.0.total(jiff::Unit::Second).ok()?;
        registers::to_raw(register, seconds)
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

/// A duration that the register holds as a whole number of minutes.
pub(crate) struct MinutesValue(jiff::Span);
impl PropertyValue for MinutesValue {
    fn description() -> HomiePropertyDescription {
        duration()
    }
    fn value(&self) -> String {
        self.0.to_string()
    }
    fn to_raw(&self, register: Register) -> Option<u16> {
        let minutes = self.0.total(jiff::Unit::Minute).ok()?;
        registers::to_raw(register, minutes)
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
