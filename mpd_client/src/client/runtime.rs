use std::fmt;

use mpd_protocol::{
    AsyncConnection, MpdProtocolError,
    command::{Command as RawCommand, CommandList as RawCommandList},
    response::Response,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
use tracing::{Instrument, Level, debug, error, span, trace};

use crate::client::{CommandResponder, ConnectionError, ConnectionEvent, Subsystem};

struct ControlState<C> {
    connection: AsyncConnection<C>,
    loop_state: ControlLoopState,
    commands: UnboundedReceiver<(RawCommandList, CommandResponder)>,
}

struct IdleState<C> {
    connection: AsyncConnection<C>,
    events: UnboundedSender<ConnectionEvent>,
}

enum ControlLoopState {
    WaitingForCommand,
    WaitingForCommandReply(CommandResponder),
}

impl fmt::Debug for ControlLoopState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // avoid Debug-printing the noisy internals of the contained channel type
        match self {
            ControlLoopState::WaitingForCommand => write!(f, "WaitingForCommand"),
            ControlLoopState::WaitingForCommandReply(_) => write!(f, "WaitingForCommandReply"),
        }
    }
}

fn idle() -> RawCommand {
    RawCommand::new("idle")
}

fn cancel_idle() -> RawCommand {
    RawCommand::new("noidle")
}

pub(super) async fn run_control_loop<C>(
    connection: AsyncConnection<C>,
    commands: UnboundedReceiver<(RawCommandList, CommandResponder)>,
) where
    C: AsyncRead + AsyncWrite + Unpin,
{
    let mut state = ControlState {
        connection,
        commands,
        loop_state: ControlLoopState::WaitingForCommand,
    };

    trace!("entering control loop");

    loop {
        let span = span!(Level::TRACE, "iteration", state = ?state.loop_state);

        match run_control_loop_iteration(state).instrument(span).await {
            Ok(new_state) => state = new_state,
            Err(()) => break,
        }
    }

    trace!("exited run_loop");
}

pub(super) async fn run_idle_loop<C>(
    mut connection: AsyncConnection<C>,
    events: UnboundedSender<ConnectionEvent>,
) where
    C: AsyncRead + AsyncWrite + Unpin,
{
    trace!("sending initial idle command");
    if let Err(e) = connection.send(idle()).await {
        error!(error = ?e, "failed to send initial idle command");
        let _ = events.send(ConnectionEvent::ConnectionClosed(e.into()));
        return;
    }
    
    let mut state = IdleState { connection, events };

    trace!("entering idle loop");

    loop {
        let span = span!(Level::TRACE, "iteration", state = "idle");

        match run_idle_loop_iteration(state).instrument(span).await {
            Ok(new_state) => state = new_state,
            Err(()) => break,
        }
    }

    trace!("exited run_loop");
}

async fn run_control_loop_iteration<C>(mut state: ControlState<C>) -> Result<ControlState<C>, ()>
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    match state.loop_state {
        ControlLoopState::WaitingForCommand => {
            let next_command = state.commands.recv().await;
            handle_command(&mut state, next_command).await;
        }
        ControlLoopState::WaitingForCommandReply(responder) => {
            let response = state.connection.receive().await.transpose().ok_or(())?;
            trace!("response to command received");
            let _ = responder.send(response.map_err(Into::into));
            state.loop_state = ControlLoopState::WaitingForCommand;
        }
    }

    Ok(state)
}

async fn run_idle_loop_iteration<C>(mut state: IdleState<C>) -> Result<IdleState<C>, ()>
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    let response = state.connection.receive().await;
    handle_idle_response(&mut state, response).await?;

    Ok(state)
}

async fn handle_command<C>(
    state: &mut ControlState<C>,
    command: Option<(RawCommandList, CommandResponder)>,
) -> Result<(), ()>
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    let (command, responder) = command.ok_or(())?;
    trace!(?command, "command received");

    // Actually send the command. This sets the state for the next loop
    // iteration.
    match state.connection.send_list(command).await {
        Ok(_) => state.loop_state = ControlLoopState::WaitingForCommandReply(responder),
        Err(e) => {
            error!(error = ?e, "failed to send command");
            let _ = responder.send(Err(e.into()));
            return Err(());
        }
    }

    trace!("command sent successfully");
    Ok(())
}

async fn handle_idle_response<C>(
    state: &mut IdleState<C>,
    response: Result<Option<Response>, MpdProtocolError>,
) -> Result<(), ()>
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    trace!("handling idle response");

    match response {
        Ok(Some(res)) => {
            match res.into_single_frame() {
                Ok(f) => {
                    if let Some(subsystem) = Subsystem::from_frame(f) {
                        debug!(?subsystem, "state change");
                        let _ = state
                            .events
                            .send(ConnectionEvent::SubsystemChange(subsystem));
                    }
                }
                Err(e) => {
                    error!(code = e.code, message = e.message, "idle returned an error");
                    let _ = state.events.send(ConnectionEvent::ConnectionClosed(
                        ConnectionError::InvalidResponse,
                    ));
                    return Err(());
                }
            }

            if let Err(e) = state.connection.send(idle()).await {
                error!(error = ?e, "failed to start idling after state change");
                let _ = state
                    .events
                    .send(ConnectionEvent::ConnectionClosed(e.into()));
                return Err(());
            }
        }
        Ok(None) => return Err(()), // The connection was closed
        Err(e) => {
            error!(error = ?e, "state change error");
            let _ = state
                .events
                .send(ConnectionEvent::ConnectionClosed(e.into()));
            return Err(());
        }
    }

    Ok(())
}
