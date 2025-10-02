//! The client implementation.

mod controller;
mod idler;
mod runtime;

pub use controller::ClientController;
pub use idler::ClientIdler;

use core::result::Result;
use std::{
    fmt,
    hash::{Hash, Hasher},
};

use mpd_protocol::{
    MpdProtocolError,
    response::{Error, Frame, Response as RawResponse},
};
use tokio::sync::{mpsc::UnboundedReceiver, oneshot};

use crate::responses::TypedResponseError;

type CommandResponder = oneshot::Sender<Result<RawResponse, CommandError>>;

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
