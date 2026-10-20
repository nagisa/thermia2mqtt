//! The nodes and the properties of the homie device.
//!
//! The properties here do not map 1:1 to the registers. We name things after what they mean to the
//! user and combine the registers where this makes sense (e.g. a version number is three registers,
//! a counter of hours is two.)
//!
//! The grouping into nodes follows the guide's own chapters. The nodes that need accessories (the
//! "EM" in the descriptions of the registers is for the expansion module) are not enabled unless
//! asked for.

use crate::homie::custom::{ActiveAlarmsKind, HeatCurveKind, running_demands, unit_bits};
use crate::homie::node::{Node, PropertyEntry, properties};
use crate::homie::value::{
    BooleanValue, FloatValue, IntegerValue, MinutesValue, SecondsValue, coefficient_of_performance,
    json, meter_energy, operating_time, split_energy, string_enum, unit, version,
};
use crate::registers::{Register, Value};
use homie5::HomieID;

string_enum! {
    #[repr(u16)]
    #[derive(Clone, Copy)]
    enum OperationalMode {
        Off = 1,
        Standby = 2,
        On = 3,
    }
}

string_enum! {
    #[repr(u16)]
    #[derive(Clone, Copy)]
    enum Demand {
        ManualOperation = 1,
        Defrost = 2,
        HotWater = 3,
        Heat = 4,
        ActiveCooling = 5,
        Pool = 6,
        AntiLegionella = 7,
        PassiveCooling = 8,
        Standby = 98,
        NoDemand = 99,
        Off = 100,
    }
}

// The guide names the modes differently in different registers: what the mode in effect calls
// "EVU" and "Comfort", the requested mode calls "EVU" and "Load-Up". Both are named the same here
// because they are the value and the `$target` of one property.
string_enum! {
    #[repr(u16)]
    #[derive(Clone, Copy)]
    enum SmartGridMode {
        Blocked = 1,
        Normal = 4,
        LoadUp = 5,
        Boost = 6,
    }
}

// The smart grid mode that the BMS asks for, when the heat pump is set up for SG-Ready.
string_enum! {
    #[repr(u16)]
    #[derive(Clone, Copy)]
    enum SmartGridRequest {
        Normal = 0,
        LoadUp = 1,
        Blocked = 2,
        Boost = 3,
    }
}

// The mode that the BMS asks for, when the heat pump is set up for power limitation and load-up
// (PL/LU) rather than SG-Ready. The guide gives 3 the same meaning as 1, which reads as `unknown`.
string_enum! {
    #[repr(u16)]
    #[derive(Clone, Copy)]
    enum PowerLimitMode {
        Normal = 0,
        Limit = 1,
        LoadUp = 2,
    }
}

string_enum! {
    #[repr(u16)]
    #[derive(Clone, Copy)]
    enum OutdoorTemperatureSource {
        Sensor = 0,
        Bms = 1,
    }
}

string_enum! {
    #[repr(u16)]
    #[derive(Clone, Copy)]
    enum MixValveMode {
        Heat = 0,
        Cool = 1,
        Auto = 2,
    }
}

string_enum! {
    #[repr(u16)]
    #[derive(Clone, Copy)]
    enum ImmersionHeater {
        Disabled = 0,
        Internal = 2,
    }
}

/// The names of the demands, without those that say there is nothing to do.
fn demand_names(values: &[Value]) -> Vec<&'static str> {
    let demands = values.iter().map(|&value| Demand::from(value));
    demands.filter(|demand| !matches!(demand, Demand::NoDemand)).map(|d| (&d).into()).collect()
}

/// The running demands out of the registers that hold them, from the most to the least important.
fn running_demand_list(first: Value, second: Value, third: Value) -> Result<String, ()> {
    Ok(json(&demand_names(&[first, second, third])))
}

fn queued_demand_list(
    first: Value,
    second: Value,
    third: Value,
    fourth: Value,
    fifth: Value,
) -> Result<String, ()> {
    Ok(json(&demand_names(&[first, second, third, fourth, fifth])))
}

properties! { static ALARMS = [
    { "any": BooleanValue = register discrete_input(66) },
    { "active": custom ActiveAlarmsKind },
    { "class-a": BooleanValue = register discrete_input(0) },
    { "class-b": BooleanValue = register discrete_input(1) },
    { "class-c": BooleanValue = register discrete_input(2) },
    { "class-d": BooleanValue = register discrete_input(3) },
    { "class-e": BooleanValue = register discrete_input(4) },
    { "reset": action = &[(Register::coil(3), 1)] },
] }

properties! { static SYSTEM = [
    { "mode": OperationalMode = register holding(0) },
    { "running-demand": Demand = register input(1) },
    { "running-demands": aggregate(string_list) = running_demands(input(2)) },
    { "running-demands-by-priority": aggregate(string_list) = running_demand_list(input(1), input(55), input(56)) },
    { "queued-demands": aggregate(string_list) = queued_demand_list(input(62), input(63), input(64), input(65), input(66)) },
    { "software-version": aggregate(text) = version(input(57), input(58), input(59)) },
    { "control-software-version": aggregate(text) = version(input(311), input(312), input(313)) },
    { "outdoor-temperature": FloatValue = register input(13) },
    { "outdoor-temperature-source": OutdoorTemperatureSource = register holding(117) },
    { "outdoor-temperature-override": FloatValue = register holding(118) },
    { "room-temperature": FloatValue = register input(121) },
    { "external-alarm-input": BooleanValue = register discrete_input(202) },
] }

properties! { static HEAT_PUMP = [
    { "compressor-running": BooleanValue = register discrete_input(199) },
    { "compressor-speed": FloatValue = register input(54) },
    { "compressor-speed-rpm": IntegerValue<unit::Rpm> = register input(5) },
    { "compressor-gear": FloatValue = register input(61) },
    { "compressor-available-gears": FloatValue = register input(4) },
    { "compressor-start-delay": SecondsValue = register input(60) },
    { "compressor-operating-time": aggregate(duration) = operating_time(input(48), input(49)) },
    { "compressor-unable-to-speed-up": BooleanValue = register discrete_input(246) },
    { "stopping": BooleanValue = register discrete_input(224) },
    { "ready-to-start": BooleanValue = register discrete_input(225) },
    { "oil-boost": BooleanValue = register discrete_input(86) },
    { "operating-pressure-limit": BooleanValue = register discrete_input(12) },
    { "min-gear-heating": IntegerValue = register holding(26) },
    { "max-gear-heating": IntegerValue = register holding(27) },
    { "min-gear-tap-water": IntegerValue = register holding(29) },
    { "max-gear-tap-water": IntegerValue = register holding(28) },
    { "gear-shift-delay-heating": MinutesValue = register holding(61) },
    { "desired-gear-heating": IntegerValue = register input(140) },
    { "desired-gear-tap-water": IntegerValue = register input(139) },
    { "discharge-pipe-temperature": FloatValue = register input(7) },
    { "liquid-line-temperature": FloatValue = register input(129) },
    { "suction-gas-temperature": FloatValue = register input(130) },
    { "inverter-temperature": IntegerValue = register input(319) },
    { "condenser-in-temperature": FloatValue = register input(8) },
    { "condenser-out-temperature": FloatValue = register input(9) },
    { "condenser-pump-running": BooleanValue = register discrete_input(210) },
    { "condenser-pump-speed": FloatValue = register input(39) },
    { "condenser-pump-min-speed": FloatValue = register holding(76) },
    { "condenser-pump-max-speed": FloatValue = register holding(79) },
    { "condenser-pump-standby-speed": FloatValue = register holding(81) },
    { "condenser-pump-variable-speed": BooleanValue = register coil(34) },
    { "condenser-pump-continuous-operation": BooleanValue = register coil(59) },
    { "brine-in-temperature": FloatValue = register input(10) },
    { "brine-out-temperature": FloatValue = register input(11) },
    { "brine-pump-running": BooleanValue = register discrete_input(218) },
    { "brine-pump-speed": FloatValue = register input(44) },
    { "brine-pump-min-speed": FloatValue = register holding(77) },
    { "brine-pump-max-speed": FloatValue = register holding(80) },
    { "brine-pump-standby-speed": FloatValue = register holding(82) },
    { "brine-pump-variable-speed": BooleanValue = register coil(35) },
    { "brine-pump-continuous-operation": BooleanValue = register coil(21) },
    { "brine-pump-external-start": BooleanValue = register discrete_input(78) },
    { "brine-pump-external-relay": BooleanValue = register discrete_input(79) },
    { "brine-in-monitoring": BooleanValue = register coil(40) },
    { "brine-out-monitoring": BooleanValue = register coil(20) },
    { "brine-in-high-alarm-limit": FloatValue = register holding(67) },
    { "brine-in-low-alarm-limit": FloatValue = register holding(68) },
    { "brine-out-low-alarm-limit": FloatValue = register holding(69) },
    { "brine-max-delta-limit": FloatValue = register holding(70) },
    { "flow-switch-enabled": BooleanValue = register coil(7) },
    { "evaporator-freeze-protection-enabled": BooleanValue = register coil(42) },
    { "system-circulation-pump-enabled": BooleanValue = register coil(22) },
    { "low-pressure": FloatValue = register input(127) },
    { "high-pressure": FloatValue = register input(128) },
    { "bubble-point-temperature": FloatValue = register input(122) },
    { "high-pressure-dew-point-temperature": FloatValue = register input(123) },
    { "low-pressure-dew-point-temperature": FloatValue = register input(124) },
    { "superheat": FloatValue = register input(125) },
    { "sub-cooling": FloatValue = register input(126) },
    { "expansion-valve-opening": FloatValue = register input(315) },
    { "gear-regulation-p": FloatValue = register input(132) },
    { "gear-regulation-i": FloatValue = register input(133) },
    { "gear-regulation-d": FloatValue = register input(134) },
    { "buffer-tank-regulation-p": FloatValue = register input(136) },
    { "buffer-tank-regulation-i": FloatValue = register input(135) },
] }

properties! { static HEATING = [
    { "enabled": BooleanValue = register coil(9) },
    { "season-active": BooleanValue = register discrete_input(220) },
    { "season-stop-temperature": FloatValue = register holding(16) },
    { "season-integral": IntegerValue = register input(131) },
    { "circulation-pump": BooleanValue = register discrete_input(211) },
    { "supply-temperature": FloatValue = register input(12) },
    { "supply-setpoint": FloatValue = register input(18) },
    { "return-temperature": FloatValue = register input(27) },
    { "calculated-demand": FloatValue = register input(30) },
    { "selected-heat-curve": FloatValue = register input(19) },
    { "heat-curve": custom HeatCurveKind::new(6) },
    { "max-supply-temperature": FloatValue = register holding(3) },
    { "min-supply-temperature": FloatValue = register holding(4) },
    { "comfort-wheel": FloatValue = register holding(5) },
    { "fixed-supply-setpoint-enabled": BooleanValue = register coil(41) },
    { "fixed-supply-setpoint": FloatValue = register holding(116) },
    { "buffer-tank-temperature": FloatValue = register input(41) },
    { "buffer-tank-setpoint": FloatValue = register input(68) },
] }

properties! { static ADDITIONAL_HEATER = [
    { "enabled": BooleanValue = register coil(5) },
    { "without-compressor": BooleanValue = register coil(25) },
    { "outdoor-temperature-dependent": BooleanValue = register coil(39) },
    { "active": BooleanValue = register discrete_input(221) },
    { "control-signal": BooleanValue = register discrete_input(206) },
    { "circulation-pump": BooleanValue = register discrete_input(219) },
    { "demand": FloatValue = register input(6) },
    { "start-threshold": FloatValue = register holding(75) },
    { "stop-threshold": FloatValue = register holding(78) },
    { "outdoor-temperature-limit": FloatValue = register holding(304) },
    { "operating-time": aggregate(duration) = operating_time(input(52), input(53)) },
] }

properties! { static TAP_WATER = [
    { "enabled": BooleanValue = register coil(8) },
    { "top-temperature": FloatValue = register input(15) },
    { "lower-temperature": FloatValue = register input(16) },
    { "weighted-temperature": FloatValue = register input(17) },
    { "start-temperature": FloatValue = register holding(22) },
    { "stop-temperature": FloatValue = register holding(23) },
    { "directional-valve-position": FloatValue = register input(47) },
    { "operating-time": aggregate(duration) = operating_time(input(50), input(51)) },
] }

properties! { static MIX_VALVE_1 = [
    { "enabled": BooleanValue = register coil(11) },
    { "mode": MixValveMode = register holding(298) },
    { "supply-temperature": FloatValue = register input(40) },
    { "supply-setpoint": FloatValue = register input(147) },
    { "position": FloatValue = register input(43) },
    { "circulation-pump": BooleanValue = register discrete_input(209) },
    { "min-supply-temperature": FloatValue = register holding(107) },
    { "max-supply-temperature": FloatValue = register holding(108) },
    { "heat-curve": custom HeatCurveKind::new(109) },
    { "outdoor-temperature-dependent-cooling": BooleanValue = register coil(37) },
    { "brine-pump-during-cooling": BooleanValue = register coil(38) },
    { "producing-passive-cooling": BooleanValue = register discrete_input(245) },
    { "passive-cooling-supply-setpoint": FloatValue = register holding(302) },
] }

/// The mixing valves of the expansion module. The registers differ from valve to valve, but not in
/// what they are.
macro_rules! em_mix_valve {
    (static $static:ident = {
        enabled: $enabled:literal,
        outdoor_dependent_cooling: $outdoor_cooling:literal,
        dew_point_calculation: $dew_point:literal,
        outdoor_dependent_heating: $outdoor_heating:literal,
        brine_pump_during_cooling: $brine_pump:literal,
        position: $position:literal,
        supply_temperature: $supply:literal,
        return_temperature: $return:literal,
        supply_setpoint: $setpoint:literal,
        min_supply_temperature: $min_supply:literal,
        max_supply_temperature: $max_supply:literal,
        heat_curve: $curve:literal,
        mode: $mode:literal,
        cooling_setpoint: $cooling_setpoint:literal,
        seasonal_cooling_temperature: $seasonal_cooling:literal,
        seasonal_heating_temperature: $seasonal_heating:literal,
        min_position: $min_position:literal,
        max_position: $max_position:literal,
    }) => {
        properties! { static $static = [
            { "enabled": BooleanValue = register coil($enabled) },
            { "mode": MixValveMode = register holding($mode) },
            { "supply-temperature": FloatValue = register input($supply) },
            { "return-temperature": FloatValue = register input($return) },
            { "supply-setpoint": FloatValue = register input($setpoint) },
            { "position": FloatValue = register input($position) },
            { "min-position": FloatValue = register holding($min_position) },
            { "max-position": FloatValue = register holding($max_position) },
            { "min-supply-temperature": FloatValue = register holding($min_supply) },
            { "max-supply-temperature": FloatValue = register holding($max_supply) },
            { "heat-curve": custom HeatCurveKind::new($curve) },
            { "outdoor-temperature-dependent-heating": BooleanValue = register coil($outdoor_heating) },
            { "outdoor-temperature-dependent-cooling": BooleanValue = register coil($outdoor_cooling) },
            { "dew-point-calculation": BooleanValue = register coil($dew_point) },
            { "brine-pump-during-cooling": BooleanValue = register coil($brine_pump) },
            { "cooling-setpoint": FloatValue = register holding($cooling_setpoint) },
            { "seasonal-cooling-temperature": FloatValue = register holding($seasonal_cooling) },
            { "seasonal-heating-temperature": FloatValue = register holding($seasonal_heating) },
        ] }
    };
}

em_mix_valve! { static MIX_VALVE_2 = {
    enabled: 16, outdoor_dependent_cooling: 43, dew_point_calculation: 44,
    outdoor_dependent_heating: 45, brine_pump_during_cooling: 55, position: 87,
    supply_temperature: 99, return_temperature: 109, supply_setpoint: 148,
    min_supply_temperature: 199, max_supply_temperature: 200, heat_curve: 201,
    mode: 305, cooling_setpoint: 306, seasonal_cooling_temperature: 307,
    seasonal_heating_temperature: 308, min_position: 40, max_position: 41,
} }

em_mix_valve! { static MIX_VALVE_3 = {
    enabled: 17, outdoor_dependent_cooling: 46, dew_point_calculation: 47,
    outdoor_dependent_heating: 48, brine_pump_during_cooling: 56, position: 88,
    supply_temperature: 100, return_temperature: 111, supply_setpoint: 149,
    min_supply_temperature: 208, max_supply_temperature: 209, heat_curve: 210,
    mode: 309, cooling_setpoint: 310, seasonal_cooling_temperature: 311,
    seasonal_heating_temperature: 312, min_position: 42, max_position: 43,
} }

em_mix_valve! { static MIX_VALVE_4 = {
    enabled: 18, outdoor_dependent_cooling: 49, dew_point_calculation: 50,
    outdoor_dependent_heating: 51, brine_pump_during_cooling: 57, position: 89,
    supply_temperature: 101, return_temperature: 113, supply_setpoint: 150,
    min_supply_temperature: 239, max_supply_temperature: 240, heat_curve: 241,
    mode: 313, cooling_setpoint: 314, seasonal_cooling_temperature: 315,
    seasonal_heating_temperature: 316, min_position: 44, max_position: 45,
} }

em_mix_valve! { static MIX_VALVE_5 = {
    enabled: 19, outdoor_dependent_cooling: 52, dew_point_calculation: 53,
    outdoor_dependent_heating: 54, brine_pump_during_cooling: 58, position: 90,
    supply_temperature: 107, return_temperature: 115, supply_setpoint: 151,
    min_supply_temperature: 248, max_supply_temperature: 249, heat_curve: 250,
    mode: 317, cooling_setpoint: 318, seasonal_cooling_temperature: 319,
    seasonal_heating_temperature: 320, min_position: 46, max_position: 47,
} }

properties! { static COOLING = [
    { "active-cooling-enabled": BooleanValue = register coil(10) },
    { "passive-cooling-enabled": BooleanValue = register coil(33) },
    { "dew-point-calculation-enabled": BooleanValue = register coil(23) },
    { "season-integral": IntegerValue = register input(36) },
    { "start-temperature": FloatValue = register holding(105) },
    { "stop-temperature": FloatValue = register holding(106) },
    { "min-outdoor-temperature": FloatValue = register holding(303) },
    { "dew-point-room": FloatValue = register input(91) },
    { "tank-temperature": FloatValue = register input(104) },
    { "tank-return-temperature": FloatValue = register input(105) },
    { "circuit-supply-temperature": FloatValue = register input(106) },
    { "circuit-return-temperature": FloatValue = register input(103) },
    { "circuit-circulation-pump": BooleanValue = register discrete_input(236) },
    { "circuit-regulation-signal": BooleanValue = register discrete_input(240) },
    { "active-cooling-valve": BooleanValue = register discrete_input(242) },
    { "passive-cooling-valve": BooleanValue = register discrete_input(243) },
    { "mix-valve-position": FloatValue = register input(92) },
    { "mix-valve-opening-degree": IntegerValue = register input(137) },
    { "mix-valve-setpoint": FloatValue = register holding(30) },
    { "mix-valve-min-position": FloatValue = register holding(49) },
    { "mix-valve-max-position": FloatValue = register holding(50) },
    { "desired-gear": IntegerValue = register input(141) },
    { "min-gear": IntegerValue = register holding(87) },
    { "max-gear": IntegerValue = register holding(88) },
    { "gear-shift-delay": MinutesValue = register holding(63) },
] }

properties! { static POOL = [
    { "enabled": BooleanValue = register coil(28) },
    { "external-heater-enabled": BooleanValue = register coil(31) },
    { "external-stop": BooleanValue = register discrete_input(77) },
    { "directional-valve": BooleanValue = register discrete_input(235) },
    { "circulation-pump": BooleanValue = register discrete_input(237) },
    { "regulation-signal": BooleanValue = register discrete_input(244) },
    { "supply-temperature": FloatValue = register input(119) },
    { "return-temperature": FloatValue = register input(120) },
    { "mix-valve-position": FloatValue = register input(94) },
    { "mix-valve-min-position": FloatValue = register holding(59) },
    { "mix-valve-max-position": FloatValue = register holding(60) },
    { "charge-setpoint": FloatValue = register holding(58) },
    { "return-setpoint": FloatValue = register holding(299) },
    { "hysteresis": FloatValue = register holding(300) },
    { "desired-gear": IntegerValue = register input(142) },
    { "min-gear": IntegerValue = register holding(85) },
    { "max-gear": IntegerValue = register holding(86) },
    { "gear-shift-delay": MinutesValue = register holding(62) },
] }

properties! { static TWC = [
    { "enabled": BooleanValue = register coil(12) },
    { "circulation-pump": BooleanValue = register discrete_input(230) },
    { "end-tank-heater": BooleanValue = register discrete_input(234) },
    { "valve-position": FloatValue = register input(86) },
    { "supply-temperature": FloatValue = register input(95) },
    { "return-temperature": FloatValue = register input(96) },
    { "end-tank-temperature": FloatValue = register input(98) },
    { "mix-valve-setpoint": FloatValue = register holding(31) },
    { "mix-valve-min-position": FloatValue = register holding(33) },
    { "mix-valve-max-position": FloatValue = register holding(34) },
    { "immersion-heater-start-temperature": FloatValue = register holding(35) },
    { "immersion-heater-stop-temperature": FloatValue = register holding(37) },
] }

properties! { static WCS = [
    { "enabled": BooleanValue = register coil(13) },
    { "circulation-pump": BooleanValue = register discrete_input(233) },
    { "regulation-signal": BooleanValue = register discrete_input(232) },
    { "valve-position": FloatValue = register input(85) },
    { "return-temperature": FloatValue = register input(97) },
    { "return-setpoint": FloatValue = register holding(32) },
    { "valve-min-position": FloatValue = register holding(38) },
    { "valve-max-position": FloatValue = register holding(39) },
] }

properties! { static HGW = [
    { "enabled": BooleanValue = register coil(6) },
    { "regulation-signal": BooleanValue = register discrete_input(223) },
    { "supply-temperature": FloatValue = register input(45) },
] }

properties! { static HOT_GAS_PUMP = [
    { "enabled": BooleanValue = register coil(14) },
    { "running": BooleanValue = register discrete_input(213) },
    { "end-tank-disconnected": BooleanValue = register input(152) },
    { "start-temperature": FloatValue = register holding(71) },
    { "lower-stop-temperature": FloatValue = register holding(72) },
    { "upper-stop-temperature": FloatValue = register holding(73) },
] }

properties! { static SURPLUS_HEAT = [
    { "chiller-enabled": BooleanValue = register coil(29) },
    { "borehole-enabled": BooleanValue = register coil(30) },
    { "directional-valve": BooleanValue = register discrete_input(238) },
    { "circulation-pump": BooleanValue = register discrete_input(239) },
    { "regulation-signal": BooleanValue = register discrete_input(241) },
    { "fan-speed": FloatValue = register input(93) },
    { "supply-temperature": FloatValue = register input(118) },
    { "return-temperature": FloatValue = register input(117) },
    { "chiller-setpoint": FloatValue = register holding(48) },
    { "fan-1-start-opening": FloatValue = register holding(51) },
    { "fan-2-start-opening": FloatValue = register holding(52) },
    { "fan-1-stop-opening": FloatValue = register holding(53) },
    { "fan-2-stop-opening": FloatValue = register holding(54) },
    { "min-opening": FloatValue = register holding(55) },
    { "max-opening": FloatValue = register holding(56) },
] }

properties! { static SECONDARY_UNITS = [
    { "genesis-secondaries": IntegerValue = register input(143) },
    { "legacy-secondaries": IntegerValue = register input(144) },
    { "total-distributed-gears": IntegerValue = register input(145) },
    { "max-requested-gear": IntegerValue = register input(146) },
    { "class-a-alarms": aggregate(unit_list) = unit_bits(input(162)) },
    { "class-b-alarms": aggregate(unit_list) = unit_bits(input(163)) },
    { "class-d-alarms": aggregate(unit_list) = unit_bits(input(160)) },
    { "class-e-alarms": aggregate(unit_list) = unit_bits(input(170)) },
    { "communication-lost": aggregate(unit_list) = unit_bits(input(161)) },
    { "legacy-compressors-running": aggregate(unit_list) = unit_bits(input(153)) },
    { "legacy-alarms": aggregate(unit_list) = unit_bits(input(154)) },
    { "legacy-start-signals": aggregate(unit_list) = unit_bits(input(155)) },
    { "legacy-tap-water-signals": aggregate(unit_list) = unit_bits(input(156)) },
    { "legacy-general-alarms": aggregate(unit_list) = unit_bits(input(171)) },
    { "legacy-expansion-card-lost": aggregate(unit_list) = unit_bits(input(173)) },
] }

properties! { static POWER = [
    { "output": FloatValue = register input(157) },
    { "consumption": FloatValue = register input(158) },
    { "cop": aggregate(float) = coefficient_of_performance(input(157), input(158)) },
] }

// SG-Ready: the mode is requested either by closing the contacts of the two inputs or by the BMS,
// and the inputs win. So the mode in effect is the value, and what the BMS has requested is its
// target.
properties! { static SMART_GRID = [
    { "mode": SmartGridMode = register input(82), target SmartGridRequest = holding(124) },
    { "input-1": BooleanValue = register discrete_input(201) },
    { "input-2": BooleanValue = register discrete_input(204) },
] }

properties! { static POWER_LIMIT = [
    { "mode": PowerLimitMode = register holding(124) },
    { "limit": FloatValue = register holding(125) },
] }

properties! { static STATISTICS = [
    { "heat-consumed-today": FloatValue = register input(331) },
    { "heat-delivered-today": FloatValue = register input(332) },
    { "heat-cop-today": FloatValue = register input(333) },
    { "heat-natural-today": FloatValue = register input(334) },
    { "heat-internal-heater-today": FloatValue = register input(335) },
    { "heat-consumed-year": aggregate(kilowatt_hours) = split_energy(input(350), input(351)) },
    { "heat-delivered-year": aggregate(kilowatt_hours) = split_energy(input(352), input(353)) },
    { "heat-cop-year": FloatValue = register input(354) },
    { "heat-natural-year": aggregate(kilowatt_hours) = split_energy(input(355), input(356)) },
    { "heat-internal-heater-year": aggregate(kilowatt_hours) = split_energy(input(348), input(349)) },
    { "tap-water-consumed-today": FloatValue = register input(336) },
    { "tap-water-delivered-today": FloatValue = register input(337) },
    { "tap-water-cop-today": FloatValue = register input(338) },
    { "tap-water-natural-today": FloatValue = register input(339) },
    { "tap-water-internal-heater-today": FloatValue = register input(340) },
    { "tap-water-consumed-year": aggregate(kilowatt_hours) = split_energy(input(359), input(360)) },
    { "tap-water-delivered-year": aggregate(kilowatt_hours) = split_energy(input(361), input(362)) },
    { "tap-water-cop-year": FloatValue = register input(363) },
    { "tap-water-natural-year": aggregate(kilowatt_hours) = split_energy(input(364), input(365)) },
    { "tap-water-internal-heater-year": aggregate(kilowatt_hours) = split_energy(input(357), input(358)) },
] }

properties! { static IMMERSION_HEATER = [
    { "enabled": ImmersionHeater = register holding(321) },
    { "step": IntegerValue = register input(67) },
] }

properties! { static ELECTRIC_METER = [
    { "current-limitation-enabled": BooleanValue = register coil(26) },
    { "restrict-additional-heater": BooleanValue = register coil(60) },
    { "restrict-secondary-units": BooleanValue = register coil(61) },
    { "max-phase-current": IntegerValue = register holding(119) },
    { "compressor-current-hysteresis": IntegerValue = register holding(120) },
    { "l1-current": FloatValue = register input(69) },
    { "l2-current": FloatValue = register input(70) },
    { "l3-current": FloatValue = register input(71) },
    { "l1-voltage": FloatValue = register input(72) },
    { "l2-voltage": FloatValue = register input(73) },
    { "l3-voltage": FloatValue = register input(74) },
    { "l1-l2-voltage": FloatValue = register input(75) },
    { "l2-l3-voltage": FloatValue = register input(76) },
    { "l3-l1-voltage": FloatValue = register input(77) },
    { "l1-power": IntegerValue = register input(78) },
    { "l2-power": IntegerValue = register input(79) },
    { "l3-power": IntegerValue = register input(80) },
    { "meter-value": IntegerValue = register input(81) },
    { "total-energy": aggregate(kilowatt_hours) = meter_energy(input(83), input(84)) },
] }

macro_rules! node {
    ($id:literal, $name:literal, $properties:ident) => {
        Node { id: HomieID::new_const($id), name: $name, properties: &$properties }
    };
}

static NODES: [Node; 25] = [
    node!("alarms", "alarms", ALARMS),
    node!("system", "operating mode, versions and sensors of the system", SYSTEM),
    node!("heat-pump", "the compressor and the refrigerant circuit", HEAT_PUMP),
    node!("heating", "heating system", HEATING),
    node!("additional-heater", "external additional heater", ADDITIONAL_HEATER),
    node!("tap-water", "tap water", TAP_WATER),
    node!("mix-valve-1", "mixing valve 1", MIX_VALVE_1),
    node!("mix-valve-2", "mixing valve 2 (expansion module)", MIX_VALVE_2),
    node!("mix-valve-3", "mixing valve 3 (expansion module)", MIX_VALVE_3),
    node!("mix-valve-4", "mixing valve 4 (expansion module)", MIX_VALVE_4),
    node!("mix-valve-5", "mixing valve 5 (expansion module)", MIX_VALVE_5),
    node!("cooling", "cooling", COOLING),
    node!("pool", "pool (expansion module)", POOL),
    node!("twc", "tap water circulation (expansion module)", TWC),
    node!("wcs", "WCS (expansion module)", WCS),
    node!("hgw", "HGW", HGW),
    node!("hot-gas-pump", "hot gas pump", HOT_GAS_PUMP),
    node!("surplus-heat", "surplus heat (expansion module)", SURPLUS_HEAT),
    node!("secondary-units", "secondary heat pump units", SECONDARY_UNITS),
    node!("power", "power output and consumption", POWER),
    node!("smart-grid", "SG-Ready smart grid control", SMART_GRID),
    node!(
        "power-limit",
        "power limitation and load-up (instead of SG-Ready smart grid control)",
        POWER_LIMIT
    ),
    node!("statistics", "energy statistics (registers that Thermia does not document)", STATISTICS),
    node!("immersion-heater", "internal immersion heater", IMMERSION_HEATER),
    node!(
        "electric-meter",
        "electric meter and current limitation (requires the current limiter accessory)",
        ELECTRIC_METER
    ),
];

/// All the nodes we know of.
pub(crate) fn all() -> &'static [Node] {
    &NODES
}

/// The nodes that are enabled unless asked otherwise.
///
/// Those that are not here need accessories, so they would only ever produce errors for most
/// installations.
pub(crate) const DEFAULT_NODES: &str =
    "alarms,system,smart-grid,heat-pump,heating,additional-heater,tap-water";
