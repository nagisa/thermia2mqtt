pub mod registers {
    use crate::registers::{Register, Space};

    /// Search and output known modbus registers.
    #[derive(clap::Parser)]
    pub struct Args {
        /// Only registers that have this in their address or description will be shown.
        filter: Option<String>,
        /// Only show the registers that no property of the homie device is made out of.
        #[clap(long)]
        unmapped: bool,
        #[clap(flatten)]
        output: crate::output::Args,
    }

    #[derive(thiserror::Error, Debug)]
    pub enum Error {
        #[error(transparent)]
        CreateOutput(crate::output::Error),
        #[error(transparent)]
        WriteOutput(crate::output::Error),
        #[error(transparent)]
        CommitOutput(crate::output::Error),
    }

    #[derive(serde::Serialize)]
    struct RegisterSchema {
        space: Space,
        address: u16,
        de_facto_address: u32,
        scale: u16,
        signed: bool,
        unit: &'static str,
        description: &'static str,
    }

    fn is_match(register: Register, pattern: &str) -> bool {
        let def = register.def();
        let pattern = pattern.to_lowercase();
        def.description.to_lowercase().contains(&pattern)
            || def.space.name().contains(&pattern)
            || def.address.to_string() == pattern
            || def.de_facto_address().to_string() == pattern
    }

    pub fn run(args: Args) -> Result<(), Error> {
        let mut output = args.output.to_output().map_err(Error::CreateOutput)?;
        let exposed = crate::homie::exposed_registers();
        output
            .table_headers(vec![
                "Space",
                "Address",
                "De facto",
                "Scale",
                "Signed",
                "Unit",
                "Description",
            ])
            .map_err(Error::WriteOutput)?;
        for register in Register::all() {
            if args.filter.as_deref().is_some_and(|pattern| !is_match(register, pattern)) {
                continue;
            }
            if args.unmapped && exposed.contains(&register) {
                continue;
            }
            let def = register.def();
            let record = RegisterSchema {
                space: def.space,
                address: def.address,
                de_facto_address: def.de_facto_address(),
                scale: def.scale,
                signed: def.signed,
                unit: def.unit,
                description: def.description,
            };
            output
                .result(
                    || {
                        vec![
                            def.space.to_string(),
                            def.address.to_string(),
                            def.de_facto_address().to_string(),
                            def.scale.to_string(),
                            def.signed.to_string(),
                            def.unit.to_string(),
                            def.description.to_string(),
                        ]
                    },
                    || &record,
                )
                .map_err(Error::WriteOutput)?;
        }
        output.commit().map_err(Error::CommitOutput)
    }
}

pub mod mqtt {
    use crate::homie::{self, Command};
    use crate::modbus;
    use rumqttc::v5::mqttbytes::v5::LastWill;
    use rumqttc::v5::{AsyncClient, Incoming, MqttOptions};
    use tokio::sync::mpsc;

    /// Start a Thermia Genesis to MQTT proxy which exposes a homie interface to the heat pump
    #[derive(clap::Parser)]
    pub struct Args {
        #[clap(flatten)]
        modbus: modbus::Args,
        #[clap(flatten)]
        device_args: homie::Args,

        /// How to connect to the MQTT broker.
        ///
        /// The value is expected to be provided as an URL, such as:
        /// `mqtt://location:1883?client_id=hostname` for plain text connection or
        /// `mqtts://location:1883?client_id=hostname` for TLS protected connection.
        #[clap(short = 'm', long)]
        mqtt_broker: String,

        /// To be provided together with `--mqtt-password` to use password based authentication
        /// with the broker.
        #[clap(short = 'u', long, requires = "mqtt_password")]
        mqtt_user: Option<String>,

        /// To be provided together with `--mqtt-user` to use password based authentication with
        /// the broker.
        #[clap(short = 'p', long, requires = "mqtt_user")]
        mqtt_password: Option<String>,

        /// The name of this device as seen in the MQTT topic and homie description.
        #[clap(long, default_value = "thermia")]
        device_name: String,
    }

    #[derive(thiserror::Error, Debug)]
    pub enum Error {
        #[error("MQTT connection fault")]
        MqttConnection(#[source] rumqttc::v5::ConnectionError),
        #[error("could not progress the homie device")]
        DeviceStep(#[source] crate::homie::Error),
        #[error("could not publish/initialize the homie device")]
        PublishHomieDevice(#[source] crate::homie::Error),
        #[error("specified device name {1} is invalid")]
        InvalidDeviceName(#[source] homie5::InvalidHomieIDError, String),
        #[error("MQTT broker address {1} is not valid")]
        InvalidBrokerAddress(#[source] rumqttc::v5::OptionError, String),
        #[error("could not create the homie device")]
        CreateDevice(#[source] crate::homie::Error),
        #[error("could not connect to the heat pump")]
        ConnectModbus(#[source] modbus::Error),
        #[error("communication with the heat pump failed")]
        Modbus(#[source] modbus::Error),
        #[error("communication with the heat pump has stopped unexpectedly")]
        ModbusTask(#[source] tokio::task::JoinError),
        #[error("communication with the heat pump has ended")]
        ModbusEnded,
    }

    fn modbus_ended(result: Result<Result<(), modbus::Error>, tokio::task::JoinError>) -> Error {
        match result {
            Ok(Ok(())) => Error::ModbusEnded,
            Ok(Err(e)) => Error::Modbus(e),
            Err(e) => Error::ModbusTask(e),
        }
    }

    #[tokio::main(flavor = "current_thread")]
    #[expect(clippy::result_large_err)]
    pub async fn run(args: Args) -> Result<(), Error> {
        let mut mqtt_options = MqttOptions::parse_url(&args.mqtt_broker)
            .map_err(|e| Error::InvalidBrokerAddress(e, args.mqtt_broker))?;
        if let Some((u, p)) =
            args.mqtt_user.as_ref().and_then(|u| Some((u, args.mqtt_password.as_ref()?)))
        {
            mqtt_options.set_credentials(u, p);
        }

        let nodes = homie::select_nodes(&args.device_args).map_err(Error::CreateDevice)?;
        let polls = homie::poll_groups(&nodes, &args.device_args).map_err(Error::CreateDevice)?;
        let modbus::Device { handle, updates, task: mut modbus_task } =
            modbus::connect(&args.modbus, polls).await.map_err(Error::ConnectModbus)?;

        let device_name = args
            .device_name
            .clone()
            .try_into()
            .map_err(|e| Error::InvalidDeviceName(e, args.device_name))?;
        let (protocol, last_will) =
            homie5::Homie5DeviceProtocol::new(device_name, homie5::HomieDomain::Default);
        mqtt_options.set_last_will(LastWill::new(
            last_will.topic,
            last_will.message,
            homie::convert_qos(last_will.qos),
            last_will.retain,
            None,
        ));
        let (client, mut client_loop) = AsyncClient::new(mqtt_options, 100);
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let mut device = homie::Device::new(
            args.device_args,
            client,
            protocol,
            nodes,
            handle,
            updates,
            command_rx,
        )
        .map_err(Error::CreateDevice)?;
        {
            let mut publish_future = std::pin::pin!(device.publish_device());
            loop {
                tokio::select! {
                    biased;
                    result = client_loop.poll() => {
                        result.map_err(Error::MqttConnection)?;
                    }
                    result = &mut modbus_task => return Err(modbus_ended(result)),
                    result = &mut publish_future => {
                        break result.map_err(Error::PublishHomieDevice)?;
                    }
                }
            }
        }
        'step: loop {
            // SUBTLE: `step` is not cancellation safe -- it may be in the middle of publishing
            // the resulting changes (which can block on the MQTT request queue being full, and
            // that queue is only drained by `client_loop.poll()`.) Dropping it would lose some of
            // the publishes. So the same future is kept and polled across MQTT events until it
            // completes.
            let mut step = std::pin::pin!(device.step());
            let step_result = loop {
                let mqtt_event = tokio::select! {
                    biased;
                    result = client_loop.poll() => result.map_err(Error::MqttConnection)?,
                    result = &mut modbus_task => return Err(modbus_ended(result)),
                    result = &mut step => break result,
                };
                match mqtt_event {
                    rumqttc::v5::Event::Incoming(Incoming::Publish(msg)) => {
                        match Command::try_from_mqtt_command(msg) {
                            Ok(cmd) => {
                                let Ok(()) = command_tx.send(cmd) else {
                                    tracing::warn!("command channel gone, terminating");
                                    break 'step;
                                };
                            }
                            Err(unexpected) => {
                                tracing::warn!(?unexpected, "unexpected mqtt command received")
                            }
                        }
                    }
                    // Acknowledgements for our own publishes and subscriptions (and keep-alive
                    // responses.) rumqttc takes care of the protocol exchange itself, there is
                    // nothing for us to do and they arrive for every single publish. Failures are
                    // reported as errors from `poll`.
                    rumqttc::v5::Event::Incoming(
                        Incoming::PubAck(_)
                        | Incoming::PubRec(_)
                        | Incoming::PubRel(_)
                        | Incoming::PubComp(_)
                        | Incoming::SubAck(_)
                        | Incoming::PingResp(_),
                    ) => {}
                    rumqttc::v5::Event::Incoming(event) => {
                        tracing::debug!(?event, "incoming mqtt event not handled");
                    }
                    rumqttc::v5::Event::Outgoing(_) => {}
                }
            };
            step_result.map_err(Error::DeviceStep)?;
        }
        Ok(())
    }
}
