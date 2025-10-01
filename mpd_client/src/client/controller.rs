//! The client implementation.

use core::result::Result;
use std::{
    fmt,
    hash::{Hash, Hasher},
    io,
    sync::Arc,
};

use bytes::BytesMut;
use mpd_protocol::{
    AsyncConnection, MpdProtocolError,
    command::{Command as RawCommand, CommandList as RawCommandList},
    response::{Error, Frame, Response as RawResponse},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        oneshot,
    },
};
use tracing::{Instrument, Level, debug, error, span, trace, warn};

use crate::{
    commands::{self as cmds, Command, CommandList},
    responses::TypedResponseError,
};

type CommandResponder = oneshot::Sender<Result<RawResponse, CommandError>>;

#[derive(Clone, Default)]
pub struct ClientController {
    commands_sender: Option<UnboundedSender<(RawCommandList, CommandResponder)>>,
    protocol_version: Arc<str>,
}

impl ClientController {
    pub async fn connect<C>(&self, connection: C) -> Result<Self, MpdProtocolError>
    where
        C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.do_connect(connection, None)
            .await
            .map_err(|e| match e {
                ConnectWithPasswordError::ProtocolError(e) => e,
                ConnectWithPasswordError::IncorrectPassword => unreachable!(),
            })
    }

    pub async fn connect_with_password<C>(
        &self,
        connection: C,
        password: &str,
    ) -> Result<Self, ConnectWithPasswordError>
    where
        C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.do_connect(connection, Some(password)).await
    }

    pub async fn connect_with_password_opt<C>(
        &self,
        connection: C,
        password: Option<&str>,
    ) -> Result<Self, ConnectWithPasswordError>
    where
        C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        self.do_connect(connection, password).await
    }

    pub async fn command<C>(&self, cmd: C) -> Result<C::Response, CommandError>
    where
        C: Command,
    {
        let command = cmd.command();
        let frame = self.raw_command(command).await?;
        let response = cmd.response(frame)?;
        Ok(response)
    }

    pub async fn command_list<L>(&self, list: L) -> Result<L::Response, CommandError>
    where
        L: CommandList,
    {
        let frames = match list.command_list() {
            Some(cmds) => self.raw_command_list(cmds).await?,
            None => Vec::new(),
        };

        list.responses(frames).map_err(Into::into)
    }

    pub async fn raw_command(&self, command: RawCommand) -> Result<Frame, CommandError> {
        self.do_send(RawCommandList::new(command))
            .await?
            .into_single_frame()
            .map_err(|error| CommandError::ErrorResponse {
                error,
                succesful_frames: Vec::new(),
            })
    }

    pub async fn raw_command_list(
        &self,
        commands: RawCommandList,
    ) -> Result<Vec<Frame>, CommandError> {
        debug!(?commands, "sending command");

        let res = self.do_send(commands).await?;
        let mut frames = Vec::with_capacity(res.successful_frames());

        for frame in res {
            match frame {
                Ok(f) => frames.push(f),
                Err(error) => {
                    return Err(CommandError::ErrorResponse {
                        error,
                        succesful_frames: frames,
                    });
                }
            }
        }

        Ok(frames)
    }

    //#[tracing::instrument(skip(self))]
    pub async fn album_art(
        &self,
        uri: &str,
    ) -> Result<Option<(BytesMut, Option<String>)>, CommandError> {
        debug!("loading album art");

        let mut out = BytesMut::new();
        let mut expected_size = 0;
        let mut embedded = false;
        let mut mime = None;

        // Try loadding embedded album art first
        match self.command(cmds::AlbumArtEmbedded::new(uri)).await {
            Ok(Some(resp)) => {
                out = resp.data;
                expected_size = resp.size;
                out.reserve(expected_size);
                embedded = true;
                mime = resp.mime;
                debug!(length = resp.size, ?mime, "found embedded album art");
            }
            Ok(None) => {
                debug!("readpicture command gave no result, falling back");
            }
            Err(e) => match e {
                CommandError::ErrorResponse { error, .. } if error.code == 5 => {
                    debug!("readpicture command unsupported, falling back");
                }
                e => return Err(e),
            },
        }

        if !embedded {
            if let Some(resp) = self.command(cmds::AlbumArt::new(uri)).await? {
                out = resp.data;
                expected_size = resp.size;
                out.reserve(expected_size);
                debug!(length = expected_size, "found separate file album art");
            } else {
                debug!("no embedded or separate album art found");
                return Ok(None);
            }
        }

        while out.len() < expected_size {
            let resp = if embedded {
                self.command(cmds::AlbumArtEmbedded::new(uri).offset(out.len()))
                    .await?
            } else {
                self.command(cmds::AlbumArt::new(uri).offset(out.len()))
                    .await?
            };

            if let Some(resp) = resp {
                trace!(received = resp.data.len(), progress = out.len());
                out.extend_from_slice(&resp.data);
            } else {
                warn!(progress = out.len(), "incomplete cover art response");
                return Ok(None);
            }
        }

        debug!(length = expected_size, "finished loading");

        Ok(Some((out, mime)))
    }

    /// Get the protocol version the underlying connection is using.
    pub fn protocol_version(&self) -> &str {
        self.protocol_version.as_ref()
    }

    /// Returns `true` if the connection to the server has been closed (by the server or due to an
    /// error).
    pub fn is_connection_closed(&self) -> bool {
        self.commands_sender.as_ref().unwrap().is_closed()
    }

    async fn do_send(&self, commands: RawCommandList) -> Result<RawResponse, CommandError> {
        let (tx, rx) = oneshot::channel();

        self.commands_sender
            .as_ref()
            .unwrap()
            .send((commands, tx))
            .map_err(|_| CommandError::ConnectionClosed)?;

        rx.await.map_err(|_| CommandError::ConnectionClosed)?
    }

    async fn do_connect<IO: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
        &self,
        io: IO,
        password: Option<&str>,
    ) -> Result<ClientController, ConnectWithPasswordError> {
        let span = span!(Level::DEBUG, "client connection");

        let (state_changes_sender, state_changes) = unbounded_channel();
        let (commands_sender, commands_receiver) = unbounded_channel();

        let mut connection = match AsyncConnection::connect(io).instrument(span.clone()).await {
            Ok(c) => c,
            Err(e) => {
                error!(error = ?e, "failed to perform initial handshake");
                return Err(e.into());
            }
        };

        let protocol_version = Arc::from(connection.protocol_version());

        if let Some(password) = password {
            trace!(parent: &span, "sending password");

            if let Err(e) = connection
                .send(RawCommand::new("password").argument(password.to_owned()))
                .instrument(span.clone())
                .await
            {
                error!(parent: &span, error = ?e, "failed to send password");
                return Err(e.into());
            }

            match connection.receive().instrument(span.clone()).await {
                Err(e) => {
                    error!(parent: &span, error = ?e, "failed to receive reply to password");
                    return Err(e.into());
                }
                Ok(None) => {
                    error!(
                        parent: &span,
                        "unexpected end of stream after sending password"
                    );
                    return Err(MpdProtocolError::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed while waiting for reply to password",
                    ))
                    .into());
                }
                Ok(Some(response)) if response.is_error() => {
                    error!(parent: &span, "incorrect password");
                    return Err(ConnectWithPasswordError::IncorrectPassword);
                }
                Ok(Some(_)) => {
                    trace!(parent: &span, "password accepted");
                }
            }
        }

        tokio::spawn(
            connection::run_loop(connection, commands_receiver, state_changes_sender)
                .instrument(span!(parent: &span, Level::TRACE, "run loop")),
        );

        let state_changes = ConnectionEvents(state_changes);
        let client = ClientController {
            commands_sender,
            protocol_version,
        };

        Ok((client, state_changes))
    }
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("protocol_version", &self.protocol_version)
            .finish_non_exhaustive()
    }
}

/// Errors which can occur when issuing a command.
#[derive(Debug)]
pub enum CommandError {
    /// The connection to MPD was closed cleanly
    ConnectionClosed,
    /// An underlying protocol error occurred, including IO errors
    Protocol(MpdProtocolError),
    /// Command returned an error
    ErrorResponse {
        /// The error
        error: Error,
        /// Possible successful frames in the same response, empty if not in a command list
        succesful_frames: Vec<Frame>,
    },
    /// A [typed command](crate::commands) failed to convert its response.
    InvalidTypedResponse(TypedResponseError),
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommandError::ConnectionClosed => write!(f, "the connection is closed"),
            CommandError::Protocol(_) => write!(f, "protocol error"),
            CommandError::InvalidTypedResponse(_) => {
                write!(f, "response was invalid for typed command")
            }
            CommandError::ErrorResponse {
                error,
                succesful_frames,
            } => {
                write!(
                    f,
                    "command returned an error [code {}]: {}",
                    error.code, error.message,
                )?;

                if !succesful_frames.is_empty() {
                    write!(f, " (after {} succesful frames)", succesful_frames.len())?;
                }

                Ok(())
            }
        }
    }
}

impl std::error::Error for CommandError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CommandError::Protocol(e) => Some(e),
            CommandError::InvalidTypedResponse(e) => Some(e),
            _ => None,
        }
    }
}

#[doc(hidden)]
impl From<MpdProtocolError> for CommandError {
    fn from(e: MpdProtocolError) -> Self {
        CommandError::Protocol(e)
    }
}

#[doc(hidden)]
impl From<TypedResponseError> for CommandError {
    fn from(e: TypedResponseError) -> Self {
        CommandError::InvalidTypedResponse(e)
    }
}

/// Error returned when [connecting with a password][Client::connect_with_password] fails.
#[derive(Debug)]
pub enum ConnectWithPasswordError {
    /// The provided password was not accepted by the server.
    IncorrectPassword,
    /// An unrelated protocol error occurred.
    ProtocolError(MpdProtocolError),
}

impl fmt::Display for ConnectWithPasswordError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectWithPasswordError::IncorrectPassword => write!(f, "incorrect password"),
            ConnectWithPasswordError::ProtocolError(_) => write!(f, "protocol error"),
        }
    }
}

impl std::error::Error for ConnectWithPasswordError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConnectWithPasswordError::ProtocolError(e) => Some(e),
            ConnectWithPasswordError::IncorrectPassword => None,
        }
    }
}

#[doc(hidden)]
impl From<MpdProtocolError> for ConnectWithPasswordError {
    fn from(e: MpdProtocolError) -> Self {
        ConnectWithPasswordError::ProtocolError(e)
    }
}

/// Receiver for [connection events][ConnectionEvent].
///
/// This includes notifications about state changes as well as the connection being closed,
/// possibly due to an error. If you don't care about these, you can just drop this receiver.
#[derive(Debug)]
pub struct ConnectionEvents(pub(crate) UnboundedReceiver<ConnectionEvent>);

impl ConnectionEvents {
    /// Wait for the next connection event.
    ///
    /// If this returns `None`, the connection was closed cleanly.
    pub async fn next(&mut self) -> Option<ConnectionEvent> {
        self.0.recv().await
    }
}

/// Events that occur during connection life cycle.
#[derive(Debug)]
pub enum ConnectionEvent {
    /// A change event in one of the subsystems of the server occurred.
    SubsystemChange(Subsystem),
    /// The connection was closed because of an error.
    ConnectionClosed(ConnectionError),
}

/// Subsystems of MPD which can receive state change notifications.
///
/// Derived from [the documentation](https://www.musicpd.org/doc/html/protocol.html#command-idle),
/// but also includes a catch-all to remain forward-compatible.
#[allow(missing_docs)]
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum Subsystem {
    Database,
    Message,
    Mixer,
    Options,
    Output,
    Partition,
    Player,
    /// Called `playlist` in the protocol.
    Queue,
    Sticker,
    StoredPlaylist,
    Subscription,
    Update,
    Neighbor,
    Mount,

    /// Catch-all variant used when the above variants do not match. Includes the raw subsystem
    /// from the MPD response.
    Other(Box<str>),
}

impl Subsystem {
    fn from_frame(mut r: Frame) -> Option<Subsystem> {
        r.get("changed").map(|raw| match &*raw {
            "database" => Subsystem::Database,
            "message" => Subsystem::Message,
            "mixer" => Subsystem::Mixer,
            "options" => Subsystem::Options,
            "output" => Subsystem::Output,
            "partition" => Subsystem::Partition,
            "player" => Subsystem::Player,
            "playlist" => Subsystem::Queue,
            "sticker" => Subsystem::Sticker,
            "stored_playlist" => Subsystem::StoredPlaylist,
            "subscription" => Subsystem::Subscription,
            "update" => Subsystem::Update,
            "neighbor" => Subsystem::Neighbor,
            "mount" => Subsystem::Mount,
            _ => Subsystem::Other(raw.into()),
        })
    }

    /// Returns the raw protocol name used for this subsystem.
    pub fn as_str(&self) -> &str {
        match self {
            Subsystem::Database => "database",
            Subsystem::Message => "message",
            Subsystem::Mixer => "mixer",
            Subsystem::Options => "options",
            Subsystem::Output => "output",
            Subsystem::Partition => "partition",
            Subsystem::Player => "player",
            Subsystem::Queue => "playlist",
            Subsystem::Sticker => "sticker",
            Subsystem::StoredPlaylist => "stored_playlist",
            Subsystem::Subscription => "subscription",
            Subsystem::Update => "update",
            Subsystem::Neighbor => "neighbor",
            Subsystem::Mount => "mount",
            Subsystem::Other(r) => r,
        }
    }
}

impl PartialEq for Subsystem {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for Subsystem {}

impl Hash for Subsystem {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

/// Errors which result in the connection being closed.
#[derive(Debug)]
pub enum ConnectionError {
    /// An underlying protocol error occurred, including IO errors.
    Protocol(MpdProtocolError),
    /// An invalid response was received (such as in response to the `idle` commands).
    InvalidResponse,
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionError::Protocol(_) => write!(f, "protocol error"),
            ConnectionError::InvalidResponse => write!(f, "invalid response"),
        }
    }
}

impl std::error::Error for ConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConnectionError::Protocol(e) => Some(e),
            ConnectionError::InvalidResponse => None,
        }
    }
}

impl From<MpdProtocolError> for ConnectionError {
    fn from(e: MpdProtocolError) -> Self {
        ConnectionError::Protocol(e)
    }
}
