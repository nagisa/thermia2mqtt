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

    impl RegisterSchema {
        fn new(register: Register) -> Self {
            let def = register.def();
            RegisterSchema {
                space: def.space,
                address: def.address,
                de_facto_address: def.de_facto_address(),
                scale: def.scale,
                signed: def.signed,
                unit: def.unit,
                description: def.description,
            }
        }

        fn table_row(&self) -> Vec<String> {
            vec![
                self.space.to_string(),
                self.address.to_string(),
                self.de_facto_address.to_string(),
                self.scale.to_string(),
                self.signed.to_string(),
                self.unit.to_string(),
                self.description.to_string(),
            ]
        }
    }

    fn is_match(register: Register, pattern: &str) -> bool {
        let def = register.def();
        let pattern = pattern.to_lowercase();
        def.description.to_lowercase().contains(&pattern)
            || def.space.name().contains(&pattern)
            || def.address.to_string() == pattern
            || def.de_facto_address().to_string() == pattern
    }

    pub fn run(args: Args) -> Result<(), crate::output::Error> {
        let mut output = args.output.to_output()?;
        let exposed = crate::homie::exposed_registers();
        output.table_headers(vec![
            "Space",
            "Address",
            "De facto",
            "Scale",
            "Signed",
            "Unit",
            "Description",
        ])?;
        for register in Register::all() {
            if args.filter.as_deref().is_some_and(|pattern| !is_match(register, pattern)) {
                continue;
            }
            if args.unmapped && exposed.contains(&register) {
                continue;
            }
            let record = RegisterSchema::new(register);
            output.result(|| record.table_row(), || &record)?;
        }
        output.commit()
    }
}

pub mod mqtt {
    use crate::homie::{self, Command};
    use crate::modbus;
    use rumqttc::v5::mqttbytes::v5::LastWill;
    use rumqttc::v5::{AsyncClient, Event, EventLoop, Incoming, MqttOptions};
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::time::Instant;

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
        #[error("could not progress the homie device")]
        DeviceStep(#[source] crate::homie::Error),
        #[error("specified device name {1} is invalid")]
        InvalidDeviceName(#[source] homie5::InvalidHomieIDError, String),
        #[error("MQTT broker address {1} is not valid")]
        InvalidBrokerAddress(#[source] rumqttc::v5::OptionError, String),
        #[error("could not create the homie device")]
        CreateDevice(#[source] crate::homie::Error),
        #[error("could not find the heat pump")]
        ResolveHeatPump(#[source] modbus::Error),
        #[error("communication with the heat pump has stopped unexpectedly")]
        ModbusTask(#[source] tokio::task::JoinError),
        #[error("communication with the heat pump has ended")]
        ModbusEnded,
    }

    fn modbus_ended(result: Result<(), tokio::task::JoinError>) -> Error {
        match result {
            Ok(()) => Error::ModbusEnded,
            Err(e) => Error::ModbusTask(e),
        }
    }

    /// How long to wait before trying to reach the broker again, after it has failed this many
    /// times in a row.
    fn reconnect_delay(failures: u32) -> Duration {
        Duration::from_secs(1 << failures.saturating_sub(1).min(5)).min(Duration::from_secs(30))
    }

    /// Wait until `at`, then poll the MQTT connection. Polling is what (re)connects it.
    async fn poll_at(
        client_loop: &mut EventLoop,
        at: Instant,
    ) -> Result<Event, rumqttc::v5::ConnectionError> {
        tokio::time::sleep_until(at).await;
        client_loop.poll().await
    }

    #[tokio::main(flavor = "current_thread")]
    #[expect(clippy::result_large_err)]
    pub async fn run(args: Args) -> Result<(), Error> {
        serve(args).await
    }

    async fn serve(args: Args) -> Result<(), Error> {
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
            modbus::connect(&args.modbus, polls).await.map_err(Error::ResolveHeatPump)?;

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
        );
        // The device is published the same way as when the broker comes back: the connection to it
        // is expected to come and go, and gets reestablished by polling.
        command_tx.send(Command::Republish).expect("the device holds on to the receiver");
        let mut failures = 0u32;
        let mut poll_not_before = Instant::now();
        loop {
            // SUBTLE: `step` is not cancellation safe -- it may be in the middle of publishing
            // the resulting changes (which can block on the MQTT request queue being full, and
            // that queue is only drained by `client_loop.poll()`.) Dropping it would lose some of
            // the publishes. So the same future is kept and polled across MQTT events until it
            // completes.
            let mut step = std::pin::pin!(device.step());
            let step_result = loop {
                let mqtt_event = tokio::select! {
                    biased;
                    result = poll_at(&mut client_loop, poll_not_before) => match result {
                        Ok(event) => event,
                        Err(error) => {
                            if failures == 0 {
                                tracing::warn!(
                                    error = (&error as &dyn std::error::Error),
                                    "cannot reach the MQTT broker, will keep trying"
                                );
                            } else {
                                tracing::debug!(
                                    error = (&error as &dyn std::error::Error),
                                    "could not connect to the MQTT broker"
                                );
                            }
                            failures = failures.saturating_add(1);
                            poll_not_before = Instant::now() + reconnect_delay(failures);
                            continue;
                        }
                    },
                    result = &mut modbus_task => return Err(modbus_ended(result)),
                    result = &mut step => break result,
                };
                let command = match mqtt_event {
                    Event::Incoming(Incoming::Publish(msg)) => {
                        match Command::try_from_mqtt_command(msg) {
                            Ok(cmd) => Some(cmd),
                            Err(unexpected) => {
                                tracing::warn!(?unexpected, "unexpected mqtt command received");
                                None
                            }
                        }
                    }
                    // The broker has published the last will, may have lost the retained messages
                    // and has forgotten the subscriptions.
                    Event::Incoming(Incoming::ConnAck(_)) if failures > 0 => {
                        tracing::info!("connected to the MQTT broker again");
                        failures = 0;
                        Some(Command::Republish)
                    }
                    // Acknowledgements for our own publishes and subscriptions (and keep-alive
                    // responses.) rumqttc takes care of the protocol exchange itself, there is
                    // nothing for us to do and they arrive for every single publish. Failures are
                    // reported as errors from `poll`.
                    Event::Incoming(
                        Incoming::PubAck(_)
                        | Incoming::PubRec(_)
                        | Incoming::PubRel(_)
                        | Incoming::PubComp(_)
                        | Incoming::SubAck(_)
                        | Incoming::PingResp(_),
                    ) => None,
                    Event::Incoming(event) => {
                        tracing::debug!(?event, "incoming mqtt event not handled");
                        None
                    }
                    Event::Outgoing(_) => None,
                };
                if let Some(command) = command {
                    command_tx.send(command).expect("the device holds on to the receiver");
                }
            };
            step_result.map_err(Error::DeviceStep)?;
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use clap::Parser as _;
        use std::net::SocketAddr;
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        use tokio::net::{TcpListener, TcpStream};

        type Publishes = mpsc::UnboundedReceiver<(usize, String, String)>;

        /// An address that nothing listens at.
        async fn unused_address() -> SocketAddr {
            TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap()
        }

        /// What the heat pump is up to.
        #[derive(Clone, Copy, PartialEq)]
        enum HeatPump {
            /// Responds with zeroes.
            Serving,
            /// Accepts connections and does not respond to anything.
            Silent,
            /// Gone: the connections that are open are closed, and new ones are refused.
            Gone,
        }

        /// A heat pump that has zero in all of the registers, for as long as it is `Serving`. Change
        /// what it does by sending the mode to the sender that is returned. It stays at the address
        /// of the listener.
        fn serve_heat_pump(listener: TcpListener) -> tokio::sync::watch::Sender<HeatPump> {
            let (mode_tx, mut mode) = tokio::sync::watch::channel(HeatPump::Serving);
            let address = listener.local_addr().unwrap();
            tokio::spawn(async move {
                let mut listener = Some(listener);
                loop {
                    let current = *mode.borrow_and_update();
                    if current == HeatPump::Gone {
                        listener = None;
                        if mode.changed().await.is_err() {
                            return;
                        }
                        continue;
                    }
                    let listener = match &listener {
                        Some(listener) => listener,
                        None => listener.insert(TcpListener::bind(address).await.unwrap()),
                    };
                    // Dropped, and so hung up on, once the mode changes.
                    let mut connections = tokio::task::JoinSet::new();
                    loop {
                        tokio::select! {
                            // Nobody to change the mode: it stays as it is.
                            true = async { mode.changed().await.is_ok() } => break,
                            accepted = listener.accept() => {
                                let (socket, _) = accepted.unwrap();
                                connections.spawn(serve_connection(socket, current));
                            }
                        }
                    }
                }
            });
            mode_tx
        }

        async fn serve_connection(mut socket: TcpStream, mode: HeatPump) {
            let mut request = [0; 12];
            while socket.read_exact(&mut request).await.is_ok() {
                if mode == HeatPump::Silent {
                    continue;
                }
                let count = usize::from(u16::from_be_bytes([request[10], request[11]]));
                let data = match request[7] {
                    1 | 2 => vec![0; count.div_ceil(8)],
                    _ => vec![0; count * 2],
                };
                let mut response = request[..4].to_vec();
                response.extend(((data.len() + 3) as u16).to_be_bytes());
                response.extend([request[6], request[7], data.len() as u8]);
                response.extend(data);
                if socket.write_all(&response).await.is_err() {
                    return;
                }
            }
        }

        async fn zeroed_heat_pump() -> (SocketAddr, tokio::sync::watch::Sender<HeatPump>) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            (listener.local_addr().unwrap(), serve_heat_pump(listener))
        }

        /// A broker that gets along with just enough of MQTT. It reports the publishes as
        /// (the number of the connection, topic, payload), and drops the current connection
        /// whenever something is sent to the sender that is returned. The connections that have not
        /// published anything (the bridge is free to give up on one before it is done) do not count.
        fn serve_broker(listener: TcpListener) -> (Publishes, mpsc::UnboundedSender<()>) {
            let (publishes_tx, publishes) = mpsc::unbounded_channel();
            let (drop_tx, mut drop_rx) = mpsc::unbounded_channel();
            tokio::spawn(async move {
                let mut connection = 0;
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut published = false;
                    loop {
                        let packet = tokio::select! {
                            packet = read_packet(&mut socket) => packet,
                            _ = drop_rx.recv() => None,
                        };
                        let Some((header, body)) = packet else { break };
                        let id = body.get(..2).unwrap_or_default();
                        let reply: Vec<u8> = match header >> 4 {
                            1 => vec![0x20, 3, 0, 0, 0],
                            3 => {
                                let topic_len = usize::from(u16::from_be_bytes([body[0], body[1]]));
                                let topic = String::from_utf8_lossy(&body[2..2 + topic_len]);
                                let mut payload = 2 + topic_len;
                                let qos = (header >> 1) & 3;
                                if qos > 0 {
                                    payload += 2;
                                }
                                payload += 1 + usize::from(body[payload]);
                                let payload = String::from_utf8_lossy(&body[payload..]);
                                published = true;
                                let message = (connection, topic.into(), payload.into());
                                let _ = publishes_tx.send(message);
                                let id = &body[2 + topic_len..];
                                match qos {
                                    1 => vec![0x40, 2, id[0], id[1]],
                                    2 => vec![0x50, 2, id[0], id[1]],
                                    _ => continue,
                                }
                            }
                            6 => vec![0x70, 2, id[0], id[1]],
                            12 => vec![0xd0, 0],
                            _ => continue,
                        };
                        socket.write_all(&reply).await.unwrap();
                    }
                    connection += usize::from(published);
                }
            });
            (publishes, drop_tx)
        }

        async fn fake_broker() -> (SocketAddr, Publishes, mpsc::UnboundedSender<()>) {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (publishes, drop_tx) = serve_broker(listener);
            (address, publishes, drop_tx)
        }

        async fn read_packet(socket: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
            let header = socket.read_u8().await.ok()?;
            let (mut length, mut shift) = (0, 0);
            loop {
                let byte = socket.read_u8().await.ok()?;
                length |= usize::from(byte & 0x7f) << shift;
                shift += 7;
                if byte & 0x80 == 0 {
                    break;
                }
            }
            let mut body = vec![0; length];
            socket.read_exact(&mut body).await.ok()?;
            Some((header, body))
        }

        /// The next `$state` that is published, and the number of the connection it came over.
        async fn next_state(publishes: &mut Publishes) -> (usize, String) {
            while let Some((connection, topic, payload)) = publishes.recv().await {
                if topic == "homie/5/thermia-test/$state" {
                    return (connection, payload);
                }
            }
            panic!("the broker has gone");
        }

        async fn expect_state(publishes: &mut Publishes, connection: usize, expected: &str) {
            let (from, state) = next_state(publishes).await;
            assert_eq!((from, state.as_str()), (connection, expected));
        }

        async fn until_ready(publishes: &mut Publishes, connection: usize) {
            while next_state(publishes).await != (connection, "ready".to_string()) {}
        }

        /// Run the bridge for as long as the scenario goes on. The bridge is not to end.
        async fn bridged(
            heat_pump: SocketAddr,
            broker: SocketAddr,
            scenario: impl Future<Output = ()>,
        ) {
            let args = Args::parse_from([
                "test",
                "--tcp",
                &heat_pump.to_string(),
                "--mqtt-broker",
                &format!("mqtt://{broker}?client_id=test"),
                "--device-name",
                "thermia-test",
                "--nodes",
                "alarms",
                "--poll",
                "alarms=100ms",
                "--timeout",
                "1s",
            ]);
            tokio::select! {
                result = serve(args) => panic!("the bridge has ended: {result:?}"),
                result = tokio::time::timeout(Duration::from_secs(15), scenario) => {
                    result.expect("the scenario did not play out");
                }
            }
        }

        #[tokio::test]
        async fn the_device_is_published_again_when_the_broker_comes_back() {
            let (heat_pump, _) = zeroed_heat_pump().await;
            let (broker, mut publishes, drop_connection) = fake_broker().await;
            bridged(heat_pump, broker, async {
                until_ready(&mut publishes, 0).await;
                drop_connection.send(()).unwrap();
                // The new connection starts out with nothing: no last will has been dealt with
                // and no retained messages are there.
                until_ready(&mut publishes, 1).await;
            })
            .await;
        }

        #[tokio::test]
        async fn the_device_is_disconnected_while_the_heat_pump_is_unreachable() {
            let (heat_pump, heat_pump_mode) = zeroed_heat_pump().await;
            let (broker, mut publishes, _drop) = fake_broker().await;
            bridged(heat_pump, broker, async {
                expect_state(&mut publishes, 0, "init").await;
                expect_state(&mut publishes, 0, "ready").await;
                heat_pump_mode.send_replace(HeatPump::Gone);
                expect_state(&mut publishes, 0, "disconnected").await;
                heat_pump_mode.send_replace(HeatPump::Serving);
                expect_state(&mut publishes, 0, "ready").await;
            })
            .await;
        }

        #[tokio::test]
        async fn the_device_stays_ready_while_the_heat_pump_does_not_respond() {
            let (heat_pump, heat_pump_mode) = zeroed_heat_pump().await;
            let (broker, mut publishes, _drop) = fake_broker().await;
            bridged(heat_pump, broker, async {
                expect_state(&mut publishes, 0, "init").await;
                expect_state(&mut publishes, 0, "ready").await;
                // It is connected to. Every poll runs into the timeout of a second, and there is
                // more than one of them in here.
                heat_pump_mode.send_replace(HeatPump::Silent);
                let state =
                    tokio::time::timeout(Duration::from_millis(2500), next_state(&mut publishes));
                let state = state.await;
                assert!(state.is_err(), "the state has changed: {state:?}");
            })
            .await;
        }

        #[tokio::test]
        async fn the_device_published_while_the_heat_pump_is_unreachable_is_disconnected() {
            let (heat_pump, heat_pump_mode) = zeroed_heat_pump().await;
            let (broker, mut publishes, drop_connection) = fake_broker().await;
            bridged(heat_pump, broker, async {
                until_ready(&mut publishes, 0).await;
                heat_pump_mode.send_replace(HeatPump::Gone);
                expect_state(&mut publishes, 0, "disconnected").await;
                drop_connection.send(()).unwrap();
                expect_state(&mut publishes, 1, "init").await;
                expect_state(&mut publishes, 1, "disconnected").await;
                heat_pump_mode.send_replace(HeatPump::Serving);
                expect_state(&mut publishes, 1, "ready").await;
            })
            .await;
        }

        #[tokio::test]
        async fn the_device_becomes_ready_once_the_broker_is_there() {
            let (heat_pump, _) = zeroed_heat_pump().await;
            let broker = unused_address().await;
            bridged(heat_pump, broker, async {
                tokio::time::sleep(Duration::from_millis(300)).await;
                let listener = TcpListener::bind(broker).await.unwrap();
                let (mut publishes, _drop) = serve_broker(listener);
                until_ready(&mut publishes, 0).await;
            })
            .await;
        }

        #[tokio::test]
        async fn the_device_becomes_ready_once_the_heat_pump_is_there() {
            let heat_pump = unused_address().await;
            let (broker, mut publishes, _drop) = fake_broker().await;
            bridged(heat_pump, broker, async {
                expect_state(&mut publishes, 0, "init").await;
                let state =
                    tokio::time::timeout(Duration::from_millis(300), next_state(&mut publishes));
                assert!(state.await.is_err(), "the device does not wait for the heat pump");
                serve_heat_pump(TcpListener::bind(heat_pump).await.unwrap());
                expect_state(&mut publishes, 0, "ready").await;
            })
            .await;
        }
    }
}
