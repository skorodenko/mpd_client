//! The client implementation.

use core::result::Result;
use std::{fmt, io, sync::Arc};

use mpd_protocol::{AsyncConnection, MpdProtocolError, command::Command as RawCommand};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc::{UnboundedReceiver, unbounded_channel},
};
use tracing::{Instrument, Level, span, error, trace};

use super::{ConnectWithPasswordError, ConnectionEvent, runtime};

#[derive(Default)]
pub struct ClientIdler {
    state_changes: Option<UnboundedReceiver<ConnectionEvent>>,
    protocol_version: Arc<str>,
}

impl ClientIdler {
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

    /// Get the protocol version the underlying connection is using.
    pub fn protocol_version(&self) -> &str {
        self.protocol_version.as_ref()
    }

    /// Returns `true` if the connection to the server has been closed (by the server or due to an
    /// error).
    pub fn is_connection_closed(&self) -> bool {
        self.state_changes.as_ref().unwrap().is_closed()
    }

    async fn do_connect<IO: AsyncRead + AsyncWrite + Unpin + Send + 'static>(
        &self,
        io: IO,
        password: Option<&str>,
    ) -> Result<ClientIdler, ConnectWithPasswordError> {
        let span = span!(Level::DEBUG, "client connection");

        let (state_changes_sender, state_changes) = unbounded_channel();

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
            runtime::run_idle_loop(connection, state_changes_sender)
                .instrument(span!(parent: &span, Level::TRACE, "run loop")),
        );

        let client = ClientIdler {
            state_changes: Some(state_changes),
            protocol_version,
        };

        Ok(client)
    }
}

impl fmt::Debug for ClientIdler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("protocol_version", &self.protocol_version)
            .finish_non_exhaustive()
    }
}
