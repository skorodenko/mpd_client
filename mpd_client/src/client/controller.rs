//! The client implementation.

use core::result::Result;
use std::{fmt, io, sync::Arc};

use bytes::BytesMut;
use mpd_protocol::{
    AsyncConnection, MpdProtocolError,
    command::{Command as RawCommand, CommandList as RawCommandList},
    response::{Frame, Response as RawResponse},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{
        mpsc::{UnboundedSender, unbounded_channel},
        oneshot,
    },
};
use tracing::{Instrument, Level, debug, error, span, trace, warn};

use super::{CommandError, CommandResponder, ConnectWithPasswordError, runtime};

use crate::commands::{self as cmds, Command, CommandList};

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
            runtime::run_control_loop(connection, commands_receiver)
                .instrument(span!(parent: &span, Level::TRACE, "run loop")),
        );

        let client = ClientController {
            commands_sender: Some(commands_sender),
            protocol_version,
        };

        Ok(client)
    }
}

impl fmt::Debug for ClientController {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("protocol_version", &self.protocol_version)
            .finish_non_exhaustive()
    }
}
