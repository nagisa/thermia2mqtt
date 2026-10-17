//! The homie side of things.
//!
//! This layer knows what the registers mean. It turns the contents of the register [`Snapshot`]
//! into homie properties and publishes them when they change. When a property is set it works out
//! what needs to be written to the registers.
//!
//! [`Snapshot`]: crate::modbus::Snapshot

mod custom;
mod node;
mod nodes;
mod value;

use crate::homie::node::{Node, PropertyEntry};
use crate::modbus::{self, PollGroup, Update};
use crate::registers::Register;
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt as _};
use homie5::client::{Publish, QoS, Subscription};
use homie5::device_description::HomieDeviceDescription;
use homie5::{Homie5DeviceProtocol, HomieDeviceStatus, HomieID, PropertyRef};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("could not publish the state init message")]
    PublishStateInit(#[source] rumqttc::v5::ClientError),
    #[error("could not construct the device description")]
    BuildDeviceDescription(#[source] homie5::Homie5ProtocolError),
    #[error("could not publish the device description message")]
    PublishDeviceDescription(#[source] rumqttc::v5::ClientError),
    #[error("could not figure out which topics to subscribe to")]
    ConstructTopicSubscriptions(#[source] homie5::Homie5ProtocolError),
    #[error("could not subscribe to the needed topics")]
    Subscribe(#[source] rumqttc::v5::ClientError),
    #[error("could not publish the state ready message")]
    PublishStateReady(#[source] rumqttc::v5::ClientError),
    #[error("unexpected homie device ID ({0}) encountered")]
    WrongDeviceId(HomieID),
    #[error("unknown node {0}")]
    UnknownNode(HomieID),
    #[error("unknown node property {0}/{1}")]
    UnknownProperty(HomieID, HomieID),
    #[error("the connection to the heat pump has stopped working")]
    ModbusGone,
    #[error("could not publish {1}/{2} = {3}")]
    PublishValue(#[source] rumqttc::v5::ClientError, HomieID, HomieID, String),
    #[error("requested node {0} is not known")]
    UnknownRequestedNode(HomieID),
    #[error("polling config value `{0}` has no equal sign (should follow `node/prop=duration`)")]
    SplitPollConfigEqualSign(String),
    #[error("polling config value `{0}` specifies an invalid duration")]
    PollParseDuration(#[source] humantime::DurationError, String),
    #[error("polling config value `{0}` specifies a malformed identifier")]
    PollParseHomieId(#[source] homie5::InvalidHomieIDError, String),
    #[error("polling config specifies a non-existent node `{0}`")]
    PollUnknownNode(HomieID),
    #[error("polling config specifies a non-existent node property `{0}/{1}`")]
    PollUnknownProp(HomieID, HomieID),
}

#[derive(Clone)]
struct PollConfig {
    node_id: HomieID,
    prop_id: Option<HomieID>,
    duration: humantime::Duration,
}

impl std::str::FromStr for PollConfig {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some((nodeprop, duration)) = value.split_once("=") else {
            return Err(Error::SplitPollConfigEqualSign(value.to_string()));
        };
        let duration =
            duration.parse().map_err(|e| Error::PollParseDuration(e, value.to_string()))?;
        let (node_id, prop_id) = match nodeprop.split_once("/") {
            Some((node_id, prop_id)) => (node_id, Some(prop_id)),
            None => (nodeprop, None),
        };
        let node_id = node_id.parse().map_err(|e| Error::PollParseHomieId(e, value.to_string()))?;
        let prop_id = prop_id
            .map(|v| v.parse())
            .transpose()
            .map_err(|e| Error::PollParseHomieId(e, value.to_string()))?;
        Ok(Self { node_id, prop_id, duration })
    }
}

#[derive(clap::Parser, Clone)]
#[group(id = "homie::Args")]
pub struct Args {
    /// Allow changing the settings of the heat pump.
    ///
    /// Unless this is specified all the properties are read-only and the device does not listen to
    /// any MQTT topics, so nothing is ever written to the heat pump.
    #[clap(long)]
    allow_writes: bool,

    /// Poll the registers of the properties that don't otherwise have any polling period
    /// configured every specified interval.
    #[clap(long, short = 'r', default_value = "10s")]
    poll_period: humantime::Duration,

    /// Configure polling for the provided nodes and properties on a case-by-case basis.
    ///
    /// Specified as `node=duration` or `node/property=duration`.
    #[arg(long, value_delimiter = ',', num_args = 0.., default_value = "alarms=3s")]
    poll: Vec<PollConfig>,

    /// Specify the nodes to enable.
    ///
    /// The nodes that are not enabled by default need accessories that not every installation has.
    #[clap(long, value_delimiter = ',', default_value = nodes::DEFAULT_NODES)]
    nodes: Vec<HomieID>,
}

/// The registers that some property of some node depends on.
pub(crate) fn exposed_registers() -> BTreeSet<Register> {
    let properties = nodes::all().iter().flat_map(|node| node.properties);
    properties.flat_map(|property| property.kind.registers().iter().copied()).collect()
}

/// The nodes that were asked for.
#[expect(clippy::result_large_err)]
pub(crate) fn select_nodes(args: &Args) -> Result<Vec<&'static Node>, Error> {
    args.nodes
        .iter()
        .map(|requested| {
            nodes::all()
                .iter()
                .find(|node| &node.id == requested)
                .ok_or_else(|| Error::UnknownRequestedNode(requested.clone()))
        })
        .collect()
}

/// Work out which registers to poll and how often in order to provide the nodes.
#[expect(clippy::result_large_err)]
pub(crate) fn poll_groups(
    selected: &[&'static Node],
    args: &Args,
) -> Result<Vec<PollGroup>, Error> {
    let mut periods: BTreeMap<Register, Duration> = BTreeMap::new();
    for node in selected {
        for property in node.properties {
            for &register in property.kind.registers() {
                periods.insert(register, *args.poll_period);
            }
        }
    }
    for config in &args.poll {
        let Some(node) = selected.iter().find(|n| n.id == config.node_id) else {
            if nodes::all().iter().any(|n| n.id == config.node_id) {
                // Just not enabled.
                continue;
            }
            return Err(Error::PollUnknownNode(config.node_id.clone()));
        };
        let mut matched = false;
        for property in node.properties {
            if config.prop_id.as_ref().is_some_and(|id| id != &property.prop_id) {
                continue;
            }
            matched = true;
            for &register in property.kind.registers() {
                periods.insert(register, *config.duration);
            }
        }
        if !matched {
            let prop_id = config.prop_id.clone().expect("a node always matches");
            return Err(Error::PollUnknownProp(node.id.clone(), prop_id));
        }
    }
    let mut groups: BTreeMap<Duration, Vec<Register>> = BTreeMap::new();
    for (register, period) in periods {
        groups.entry(period).or_default().push(register);
    }
    Ok(groups.into_iter().map(|(period, registers)| PollGroup { period, registers }).collect())
}

/// What happened after we've asked the heat pump to change a property.
struct SetCompleted {
    node_idx: usize,
    prop_idx: usize,
    result: Result<(), modbus::Error>,
    /// For the properties that are not backed by registers: what to tell the controller.
    acknowledgement: Option<String>,
}

type SetFuture = Pin<Box<dyn Future<Output = SetCompleted> + Send>>;

/// Which properties have a value at the moment, indexed like the properties of the nodes.
type Availability = Vec<Vec<bool>>;

pub(crate) struct Device {
    args: Args,
    mqtt: rumqttc::v5::AsyncClient,
    protocol: Homie5DeviceProtocol,
    state: HomieDeviceStatus,
    description: HomieDeviceDescription,
    nodes: Vec<&'static Node>,
    /// The properties that are a part of the description.
    ///
    /// A property is only there when its registers hold a usable value (e.g. a sensor that is
    /// not installed does not have one.) There is no way to say that a property has no value,
    /// and a device that is `ready` has to have values for all of its properties, so the ones
    /// without values are not described at all.
    available: Availability,
    /// What we have last published, indexed like the properties of `nodes`.
    published: Vec<Vec<Option<String>>>,
    modbus: modbus::Handle,
    updates: mpsc::UnboundedReceiver<Update>,
    commands: mpsc::UnboundedReceiver<Command>,
    sets: FuturesUnordered<SetFuture>,
}

fn build_description(
    args: &Args,
    nodes: &[&'static Node],
    available: &Availability,
) -> HomieDeviceDescription {
    let mut description =
        homie5::device_description::DeviceDescriptionBuilder::new().name("Thermia Genesis");
    for (node, available) in nodes.iter().zip(available) {
        if available.iter().any(|&a| a) {
            description = description.add_node(node.id.clone(), node.description(|i| available[i]));
        }
    }
    let mut description = description.build();
    if !args.allow_writes {
        for node in &mut description.nodes.values_mut() {
            for prop in &mut node.properties.values_mut() {
                prop.settable = false;
            }
        }
    }
    description
}

impl Device {
    #[expect(clippy::result_large_err)]
    pub(crate) fn new(
        args: Args,
        mqtt: rumqttc::v5::AsyncClient,
        protocol: Homie5DeviceProtocol,
        nodes: Vec<&'static Node>,
        modbus: modbus::Handle,
        updates: mpsc::UnboundedReceiver<Update>,
        commands: mpsc::UnboundedReceiver<Command>,
    ) -> Result<Self, Error> {
        // What is really available is only known once the registers have been read.
        let available: Availability =
            nodes.iter().map(|n| vec![true; n.properties.len()]).collect();
        let description = build_description(&args, &nodes, &available);
        let published = nodes.iter().map(|n| vec![None; n.properties.len()]).collect();
        Ok(Self {
            args,
            mqtt,
            state: HomieDeviceStatus::Init,
            protocol,
            description,
            nodes,
            available,
            published,
            modbus,
            updates,
            commands,
            sets: FuturesUnordered::new(),
        })
    }

    /// Which properties we can make a value for out of what the registers hold at the moment.
    fn compute_availability(&self) -> Availability {
        let snapshot = self.modbus.snapshot();
        let available = |property: &PropertyEntry| match property.kind.value_from_modbus(&snapshot)
        {
            Some(Ok(_)) => true,
            Some(Err(())) => false,
            // Actions do not have a value, but are there. For the others we are missing registers.
            None => property.kind.registers().is_empty(),
        };
        self.nodes.iter().map(|n| n.properties.iter().map(available).collect()).collect()
    }

    async fn publish_state(&mut self, state: HomieDeviceStatus) -> Result<(), Error> {
        self.state = state;
        let p = self.protocol.publish_state(state);
        let error = match state {
            HomieDeviceStatus::Ready => Error::PublishStateReady,
            _ => Error::PublishStateInit,
        };
        self.mqtt.homie_publish(p).await.map_err(error)
    }

    /// Wait until there is a value for every register we need.
    async fn wait_for_registers(&mut self) -> Result<(), Error> {
        tracing::info!("waiting for device read out…");
        let needed: BTreeSet<Register> = self
            .nodes
            .iter()
            .flat_map(|node| node.properties)
            .flat_map(|property| property.kind.registers().iter().copied())
            .collect();
        loop {
            let missing = self.modbus.snapshot().missing(needed.iter().copied());
            if missing.is_empty() {
                return Ok(());
            }
            tracing::debug!(count = missing.len(), "registers have not been read yet");
            if self.updates.recv().await.is_none() {
                return Err(Error::ModbusGone);
            }
        }
    }

    /// Publish the description, and then the values of the properties that it describes.
    ///
    /// Must be done while the device is not `ready`.
    async fn publish_properties(&mut self) -> Result<(), Error> {
        let available = self.compute_availability();
        for (node, available) in self.nodes.iter().zip(&available) {
            for (property, _) in node.properties.iter().zip(available).filter(|(_, a)| !**a) {
                tracing::info!(
                    node_id = %node.id,
                    prop_id = %property.prop_id,
                    "the heat pump has no value for the property, it is not described"
                );
            }
        }
        self.available = available;
        self.description = build_description(&self.args, &self.nodes, &self.available);
        let p = self
            .protocol
            .publish_description(&self.description)
            .map_err(Error::BuildDeviceDescription)?;
        self.mqtt.homie_publish(p).await.map_err(Error::PublishDeviceDescription)?;

        for node_idx in 0..self.nodes.len() {
            for prop_idx in 0..self.nodes[node_idx].properties.len() {
                self.published[node_idx][prop_idx] = None;
                if self.available[node_idx][prop_idx] {
                    self.refresh(node_idx, prop_idx, true).await?;
                } else {
                    // Nothing may be left from before the property went away (or from a run that
                    // had a value for it.)
                    self.erase_value(node_idx, prop_idx).await?;
                }
            }
        }
        // rumqttc appears to be sending publishes in a weird order that results in some of the
        // properties getting published *after* `$state = Ready`. Yielding here gives it *some*
        // time to do it's thing.
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn subscribe(&mut self) -> Result<(), Error> {
        // SUBTLE: rumqttc balks at empty list of subscriptions, but that can happen if we don't
        // really have any settable properties...
        if !self.args.allow_writes {
            return Ok(());
        }
        let mut p = self
            .protocol
            .subscribe_props(&self.description)
            .map_err(Error::ConstructTopicSubscriptions)?
            .peekable();
        if p.peek().is_some() {
            self.mqtt.homie_subscribe(p).await.map_err(Error::Subscribe)?;
        }
        Ok(())
    }

    pub(crate) async fn publish_device(&mut self) -> Result<(), Error> {
        self.publish_state(HomieDeviceStatus::Init).await?;
        // The description depends on what the heat pump has to say.
        self.wait_for_registers().await?;
        self.publish_properties().await?;
        self.subscribe().await?;
        self.publish_state(HomieDeviceStatus::Ready).await?;
        tracing::debug!("device became ready...");
        Ok(())
    }

    /// Describe the properties again, as some have gained or lost their values.
    ///
    /// The description may only change while the device is not `ready`.
    async fn reconfigure(&mut self) -> Result<(), Error> {
        tracing::info!("the properties that have values have changed, describing the device again");
        self.publish_state(HomieDeviceStatus::Init).await?;
        self.publish_properties().await?;
        self.subscribe().await?;
        self.publish_state(HomieDeviceStatus::Ready).await
    }

    /// Publish the current value of a property, if it is a different one from what we have
    /// published last (or `force` is set.)
    async fn refresh(
        &mut self,
        node_idx: usize,
        prop_idx: usize,
        force: bool,
    ) -> Result<(), Error> {
        let node: &'static Node = self.nodes[node_idx];
        let property = &node.properties[prop_idx];
        if !self.available[node_idx][prop_idx] {
            return Ok(());
        }
        let value = {
            let snapshot = self.modbus.snapshot();
            property.kind.value_from_modbus(&snapshot)
        };
        let value = match value {
            Some(Ok(value)) => value.value(),
            // An action, or something that is going to be dealt with by describing the device
            // again.
            None | Some(Err(())) => return Ok(()),
        };
        if !force && self.published[node_idx][prop_idx].as_deref() == Some(value.as_str()) {
            return Ok(());
        }
        self.publish_value(node_idx, prop_idx, &value).await?;
        self.published[node_idx][prop_idx] = Some(value);
        Ok(())
    }

    async fn publish_value(
        &mut self,
        node_idx: usize,
        prop_idx: usize,
        value: &str,
    ) -> Result<(), Error> {
        let node: &'static Node = self.nodes[node_idx];
        let prop_id = &node.properties[prop_idx].prop_id;
        let Some(description) = self.description.get_property_by_id(&node.id, prop_id) else {
            tracing::warn!(node_id = %node.id, %prop_id, "property without description");
            return Ok(());
        };
        let message = self.protocol.publish_value(
            &node.id,
            prop_id,
            &value.to_string(),
            description.retained,
        );
        self.mqtt
            .homie_publish(message)
            .await
            .map_err(|e| Error::PublishValue(e, node.id.clone(), prop_id.clone(), value.into()))
    }

    /// Delete the (retained) value of a property that is not described.
    async fn erase_value(&mut self, node_idx: usize, prop_idx: usize) -> Result<(), Error> {
        let node: &'static Node = self.nodes[node_idx];
        let prop_id = &node.properties[prop_idx].prop_id;
        let mut message = self.protocol.publish_value(&node.id, prop_id, &String::new(), true);
        // An empty payload is how MQTT deletes retained messages.
        message.payload.clear();
        self.mqtt
            .homie_publish(message)
            .await
            .map_err(|e| Error::PublishValue(e, node.id.clone(), prop_id.clone(), String::new()))
    }

    async fn handle_update(&mut self, update: Update) -> Result<(), Error> {
        // A sensor can come and go. That is a change to what is described, not to a value.
        if self.compute_availability() != self.available {
            return self.reconfigure().await;
        }
        let changed: HashSet<Register> = update.changed.into_iter().collect();
        // We don't try to be smart: anything that depends on a register that has changed is
        // recomputed, and gets published if the result is different.
        for node_idx in 0..self.nodes.len() {
            for prop_idx in 0..self.nodes[node_idx].properties.len() {
                let registers = self.nodes[node_idx].properties[prop_idx].kind.registers();
                if registers.iter().any(|r| changed.contains(r)) {
                    self.refresh(node_idx, prop_idx, false).await?;
                }
            }
        }
        Ok(())
    }

    async fn handle_command(&mut self, command: Command) -> Result<(), Error> {
        if self.state != HomieDeviceStatus::Ready {
            tracing::debug!(?command, "command ignored, device is not ready yet");
            return Ok(());
        }
        let Command::Set { property, value } = command;
        if !self.args.allow_writes {
            tracing::warn!(?property, "ignoring a set command, writes are not allowed");
            return Ok(());
        }
        if property.device_id() != self.protocol.device_ref().device_id() {
            return Err(Error::WrongDeviceId(property.device_id().clone()));
        }
        let node_idx = self
            .nodes
            .iter()
            .position(|node| &node.id == property.node_id())
            .ok_or_else(|| Error::UnknownNode(property.node_id().clone()))?;
        let node: &'static Node = self.nodes[node_idx];
        let prop_idx =
            node.properties.iter().position(|p| &p.prop_id == property.prop_id()).ok_or_else(
                || Error::UnknownProperty(node.id.clone(), property.prop_id().clone()),
            )?;
        let entry = &node.properties[prop_idx];
        let (node_id, prop_id) = (&node.id, &entry.prop_id);
        if !self.available[node_idx][prop_idx] {
            tracing::warn!(%node_id, %prop_id, "ignoring a set command, the property has no value");
            return Ok(());
        }

        let writes = entry
            .kind
            .value_from_homie(&value)
            .and_then(|parsed| Ok((entry.kind.to_modbus(&*parsed)?, parsed.value())));
        let Ok((writes, parsed)) = writes else {
            tracing::warn!(%node_id, %prop_id, value, "property/set could not be understood");
            // The controller is told what the actual value is.
            return self.refresh(node_idx, prop_idx, true).await;
        };
        let modbus = self.modbus.clone();
        let acknowledgement = entry.kind.registers().is_empty().then_some(parsed);
        self.sets.push(Box::pin(async move {
            let result = modbus.write(writes).await;
            SetCompleted { node_idx, prop_idx, result, acknowledgement }
        }));
        Ok(())
    }

    async fn handle_set_completed(&mut self, completed: SetCompleted) -> Result<(), Error> {
        let SetCompleted { node_idx, prop_idx, result, acknowledgement } = completed;
        let node = self.nodes[node_idx];
        let prop_id = &node.properties[prop_idx].prop_id;
        match result {
            Ok(()) => {
                if let Some(value) = acknowledgement {
                    self.publish_value(node_idx, prop_idx, &value).await?;
                }
            }
            Err(error) => {
                tracing::error!(
                    node_id = %node.id,
                    %prop_id,
                    error = (&error as &dyn std::error::Error),
                    "could not set the property"
                );
            }
        }
        // Whatever has happened, make sure the controller knows what the property really is.
        self.refresh(node_idx, prop_idx, true).await
    }

    /// Wait for something to happen and deal with it.
    ///
    /// This is not cancellation safe: dropping the future in the middle of handling an event can
    /// lose some of the publishes.
    pub(crate) async fn step(&mut self) -> Result<(), Error> {
        tokio::select! {
            update = self.updates.recv() => {
                let Some(update) = update else { return Err(Error::ModbusGone) };
                self.handle_update(update).await
            },
            command = self.commands.recv() => {
                let Some(command) = command else { return Ok(()) };
                self.handle_command(command).await
            },
            completed = self.sets.next(), if !self.sets.is_empty() => {
                let Some(completed) = completed else { return Ok(()) };
                self.handle_set_completed(completed).await
            },
        }
    }
}

trait MqttClientExt {
    type PublishError;
    type SubscribeError;
    async fn homie_publish(&self, p: Publish) -> Result<(), Self::PublishError>;
    async fn homie_subscribe(
        &self,
        subs: impl Iterator<Item = Subscription> + Send,
    ) -> Result<(), Self::SubscribeError>;
}

impl MqttClientExt for rumqttc::v5::AsyncClient {
    type PublishError = rumqttc::v5::ClientError;
    type SubscribeError = rumqttc::v5::ClientError;
    async fn homie_publish(&self, p: Publish) -> Result<(), Self::PublishError> {
        self.publish(p.topic, convert_qos(p.qos), p.retain, p.payload).await
    }

    async fn homie_subscribe(
        &self,
        subs: impl Iterator<Item = Subscription> + Send,
    ) -> Result<(), Self::SubscribeError> {
        self.subscribe_many(
            subs.map(|sub| {
                rumqttc::v5::mqttbytes::v5::Filter::new(sub.topic, convert_qos(sub.qos))
            }),
        )
        .await
    }
}

pub fn convert_qos(homie: QoS) -> rumqttc::v5::mqttbytes::QoS {
    match homie {
        QoS::AtMostOnce => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
        QoS::AtLeastOnce => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
        QoS::ExactlyOnce => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
    }
}

#[derive(Debug)]
pub(crate) enum Command {
    Set { property: PropertyRef, value: String },
}

impl Command {
    #[expect(clippy::result_large_err)]
    pub(crate) fn try_from_mqtt_command(
        msg: rumqttc::v5::mqttbytes::v5::Publish,
    ) -> Result<Self, rumqttc::v5::mqttbytes::v5::Publish> {
        let Ok(topic) = str::from_utf8(&msg.topic) else {
            return Err(msg);
        };
        match homie5::parse_mqtt_message(topic, &msg.payload) {
            Ok(homie5::Homie5Message::PropertySet { property, set_value }) => {
                Ok(Self::Set { property, value: set_value })
            }
            _ => Err(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modbus::Snapshot;
    use crate::registers::Space;
    use clap::Parser as _;

    fn default_args() -> Args {
        Args::parse_from(["test"])
    }

    fn snapshot(words: &[(Space, u16, u16)]) -> Snapshot {
        let mut snapshot = Snapshot::new();
        for &(space, address, word) in words {
            snapshot.set(space, address, word);
        }
        snapshot
    }

    fn node(node_id: &str) -> &'static Node {
        nodes::all().iter().find(|n| n.id.to_string() == node_id).expect("node exists")
    }

    fn property(node_id: &str, prop_id: &str) -> &'static node::PropertyEntry {
        let node = node(node_id);
        node.properties.iter().find(|p| p.prop_id.to_string() == prop_id).expect("prop exists")
    }

    /// The published value of the property, as a string.
    fn value_of(node_id: &str, prop_id: &str, snapshot: &Snapshot) -> Option<String> {
        let property = property(node_id, prop_id);
        let value = property.kind.value_from_modbus(snapshot)?;
        Some(value.ok().expect("value could be computed").value())
    }

    #[test]
    fn nodes_are_well_formed() {
        let mut node_ids = BTreeSet::new();
        for node in nodes::all() {
            assert!(node_ids.insert(node.id.clone()), "{} is duplicated", node.id);
            assert_eq!(node.description(|_| true).properties.len(), node.properties.len());
            let mut prop_ids = BTreeSet::new();
            for property in node.properties {
                let name = format!("{}/{}", node.id, property.prop_id);
                assert!(prop_ids.insert(property.prop_id.clone()), "{name} is duplicated");
                let description = property.description();
                let registers = property.kind.registers();
                if description.settable {
                    // Either there's registers that can be written to, or this is an action.
                    assert!(
                        registers.iter().all(|r| r.space().is_writable()),
                        "{name} is settable, but is not backed by writable registers"
                    );
                    assert_eq!(registers.is_empty(), !description.retained, "{name}");
                } else {
                    assert!(!registers.is_empty(), "{name} is not backed by any registers");
                    assert!(description.retained, "{name}");
                }
            }
        }
    }

    #[test]
    fn nothing_is_settable_unless_allowed() {
        // The description is what makes controllers offer the controls.
        let all = nodes::all().iter().map(|n| n.id.to_string()).collect::<Vec<_>>().join(",");
        let args = Args::parse_from(["test", "--nodes", &all]);
        assert!(!args.allow_writes);
        let nodes = select_nodes(&args).unwrap();
        let (client, _) = rumqttc::v5::AsyncClient::new(
            rumqttc::v5::MqttOptions::new("test", "127.0.0.1", 1883),
            10,
        );
        let (protocol, _) = Homie5DeviceProtocol::new(
            "thermia".to_string().try_into().unwrap(),
            homie5::HomieDomain::Default,
        );
        let validation_protocol = Homie5DeviceProtocol::new(
            "thermia".to_string().try_into().unwrap(),
            homie5::HomieDomain::Default,
        )
        .0;
        let (_, updates) = mpsc::unbounded_channel();
        let (_, commands) = mpsc::unbounded_channel();
        let handle = modbus::Handle::detached(Snapshot::new());
        let device = Device::new(args, client, protocol, nodes, handle, updates, commands).unwrap();
        for node in device.description.nodes.values() {
            for (id, property) in &node.properties {
                assert!(!property.settable, "{id} is settable");
            }
        }
        // The description is acceptable to the protocol implementation.
        validation_protocol.publish_description(&device.description).expect("valid description");
    }

    #[test]
    fn values() {
        let snapshot = snapshot(&[
            (Space::Input, 13, 2150),
            (Space::Input, 12, 65436),
            (Space::Input, 57, 1),
            (Space::Input, 58, 12),
            (Space::Input, 59, 3),
            (Space::Input, 48, 2),
            (Space::Input, 49, 2345),
            (Space::Input, 1, 4),
            (Space::Input, 82, 5),
            (Space::Input, 83, 2),
            (Space::Input, 84, 1),
            (Space::Holding, 0, 3),
            (Space::Coil, 9, 1),
        ]);
        assert_eq!(value_of("system", "outdoor-temperature", &snapshot).unwrap(), "21.5");
        assert_eq!(value_of("heating", "supply-temperature", &snapshot).unwrap(), "-1");
        assert_eq!(value_of("system", "software-version", &snapshot).unwrap(), "1.12.3");
        assert_eq!(
            value_of("heat-pump", "compressor-operating-time", &snapshot).unwrap(),
            "PT133417H"
        );
        assert_eq!(value_of("system", "running-demand", &snapshot).unwrap(), "heat");
        assert_eq!(value_of("system", "comfort-mode", &snapshot).unwrap(), "comfort");
        assert_eq!(value_of("system", "mode", &snapshot).unwrap(), "on");
        assert_eq!(value_of("heating", "enabled", &snapshot).unwrap(), "true");
        // (1 << 16 | 2) / 10
        assert_eq!(value_of("electric-meter", "total-energy", &snapshot).unwrap(), "6553.8");
    }

    #[test]
    fn sensors_that_are_not_there_are_not_published() {
        // The heat pump reports 200 °C for these. 20000 is 200.00 °C and 2000 is 200.0 °C.
        let snapshot = snapshot(&[
            (Space::Input, 27, 20000),
            (Space::Input, 121, 2000),
            (Space::Holding, 118, 20000),
            (Space::Input, 13, 19999),
        ]);
        for (node_id, prop_id) in [
            ("heating", "return-temperature"),
            ("system", "room-temperature"),
            ("system", "outdoor-temperature-override"),
        ] {
            let kind = property(node_id, prop_id).kind;
            assert!(kind.value_from_modbus(&snapshot).unwrap().is_err(), "{node_id}/{prop_id}");
        }
        // Just below the sentinel is a valid temperature.
        assert_eq!(value_of("system", "outdoor-temperature", &snapshot).unwrap(), "199.99");
    }

    #[test]
    fn values_are_not_made_up_for_registers_we_have_not_seen() {
        let snapshot = snapshot(&[(Space::Input, 57, 1), (Space::Input, 58, 2)]);
        assert!(value_of("system", "software-version", &snapshot).is_none());
        assert!(value_of("system", "outdoor-temperature", &snapshot).is_none());
    }

    #[test]
    fn unknown_enum_values_are_not_published() {
        let snapshot = snapshot(&[(Space::Holding, 0, 42)]);
        let mode = property("system", "mode");
        assert!(mode.kind.value_from_modbus(&snapshot).unwrap().is_err());
    }

    #[test]
    fn active_alarms() {
        let registers = property("alarms", "active").kind.registers();
        let mut words: Vec<_> = registers.iter().map(|r| (r.space(), r.address(), 0)).collect();
        let at = |words: &mut Vec<(Space, u16, u16)>, address, word| {
            words.iter_mut().find(|w| w.1 == address).unwrap().2 = word;
        };
        at(&mut words, 9, 1); // high pressure switch
        at(&mut words, 34, 1); // outdoor sensor
        let snapshot = snapshot(&words);
        assert_eq!(
            value_of("alarms", "active", &snapshot).unwrap(),
            r#"["High pressure switch alarm","Outdoor sensor alarm"]"#
        );
        at(&mut words, 9, 0);
        at(&mut words, 34, 0);
        let snapshot = self::snapshot(&words);
        assert_eq!(value_of("alarms", "active", &snapshot).unwrap(), "[]");
    }

    #[test]
    fn heat_curve() {
        let mut words = Vec::new();
        for i in 0..7u16 {
            words.push((Space::Input, 20 + i, (i * 500).wrapping_sub(1000)));
            words.push((Space::Holding, 6 + i, 3000 + i * 100));
        }
        let snapshot = snapshot(&words);
        assert_eq!(
            value_of("heating", "heat-curve", &snapshot).unwrap(),
            "[[-10.0,30.0],[-5.0,31.0],[0.0,32.0],[5.0,33.0],[10.0,34.0],[15.0,35.0],[20.0,36.0]]"
        );
    }

    #[test]
    fn what_to_write() {
        // Only for the sake of checking the mapping, nothing is written anywhere.
        let start = property("tap-water", "start-temperature");
        let parsed = start.kind.value_from_homie("45.5").ok().unwrap();
        let writes = start.kind.to_modbus(&*parsed).ok().unwrap();
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, Register::holding(22));
        assert_eq!(writes[0].1, 4550);
        // Out of what the register can represent.
        let parsed = start.kind.value_from_homie("1000").ok().unwrap();
        assert!(start.kind.to_modbus(&*parsed).is_err());
        assert!(start.kind.value_from_homie("warm").is_err());
        // Read-only values cannot be set.
        let top = property("tap-water", "top-temperature");
        assert!(!top.description().settable);
    }

    #[test]
    fn polls_are_grouped_by_period() {
        let args = default_args();
        let selected = select_nodes(&args).unwrap();
        let groups = poll_groups(&selected, &args).unwrap();
        let mut seen = BTreeSet::new();
        for group in &groups {
            for register in &group.registers {
                assert!(seen.insert(*register), "{register:?} is in more than one group");
            }
        }
        let period = |register: Register| {
            groups.iter().find(|g| g.registers.contains(&register)).map(|g| g.period)
        };
        assert_eq!(period(Register::discrete_input(66)), Some(Duration::from_secs(3)));
        assert_eq!(period(Register::input(13)), Some(Duration::from_secs(10)));
        // Not enabled by default.
        assert_eq!(period(Register::input(69)), None);

        let mut args = default_args();
        args.poll = vec!["system/outdoor-temperature=1s".parse().ok().unwrap()];
        let groups = poll_groups(&select_nodes(&args).unwrap(), &args).unwrap();
        let period = groups.iter().find(|g| g.registers.contains(&Register::input(13)));
        assert_eq!(period.map(|g| g.period), Some(Duration::from_secs(1)));
        args.poll = vec!["system/nonsense=1s".parse().ok().unwrap()];
        assert!(poll_groups(&select_nodes(&args).unwrap(), &args).is_err());
    }

    /// A device that is not connected to anything, with the specified registers set.
    fn test_device(args: Args, words: &[(Space, u16, u16)]) -> Device {
        let nodes = select_nodes(&args).unwrap();
        let (client, _) = rumqttc::v5::AsyncClient::new(
            rumqttc::v5::MqttOptions::new("test", "127.0.0.1", 1883),
            10,
        );
        let (protocol, _) = Homie5DeviceProtocol::new(
            "thermia".to_string().try_into().unwrap(),
            homie5::HomieDomain::Default,
        );
        let (_, updates) = mpsc::unbounded_channel();
        let (_, commands) = mpsc::unbounded_channel();
        let handle = modbus::Handle::detached(snapshot(words));
        Device::new(args, client, protocol, nodes, handle, updates, commands).unwrap()
    }

    /// Every register the nodes need is 1, apart from what is overridden.
    fn all_registers(args: &Args, overrides: &[(Space, u16, u16)]) -> Vec<(Space, u16, u16)> {
        let mut words: BTreeMap<(Space, u16), u16> = BTreeMap::new();
        for node in select_nodes(args).unwrap() {
            for property in node.properties {
                for register in property.kind.registers() {
                    words.insert((register.space(), register.address()), 1);
                }
            }
        }
        for &(space, address, word) in overrides {
            words.insert((space, address), word);
        }
        words.into_iter().map(|((space, address), word)| (space, address, word)).collect()
    }

    #[test]
    fn properties_without_values_are_not_described() {
        let args = default_args();
        let sensors_missing =
            [(Space::Input, 27, 20000), (Space::Input, 121, 2000), (Space::Holding, 118, 20000)];
        let missing = test_device(args.clone(), &all_registers(&args, &sensors_missing));
        let present = test_device(args.clone(), &all_registers(&args, &[]));

        let available = missing.compute_availability();
        let described = |device: &Device, node: &str, prop: &str| {
            let (node, prop) = (node.parse::<HomieID>().unwrap(), prop.parse::<HomieID>().unwrap());
            device.description.get_property_by_id(&node, &prop).is_some()
        };
        let description = build_description(&args, &missing.nodes, &available);
        let described_in = |node: &str, prop: &str| {
            let (node, prop) = (node.parse::<HomieID>().unwrap(), prop.parse::<HomieID>().unwrap());
            description.get_property_by_id(&node, &prop).is_some()
        };
        for (node, prop) in [
            ("system", "room-temperature"),
            ("system", "outdoor-temperature-override"),
            ("heating", "return-temperature"),
        ] {
            assert!(!described_in(node, prop), "{node}/{prop} should not be described");
            assert!(described(&present, node, prop), "{node}/{prop} should be described");
        }
        assert!(described_in("system", "outdoor-temperature"));
        // Actions do not have values, but are still there.
        assert!(described_in("alarms", "reset"));
        // This is what makes the device describe itself again, when a sensor shows up.
        assert_ne!(available, present.compute_availability());
        assert_eq!(present.compute_availability(), present.available);
    }
}
