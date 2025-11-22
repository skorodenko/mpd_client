use crate::responses::TypedResponseError;
use mpd_protocol::response::Frame;

/// List all dirs with music (recursive)
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListDirs {}

impl ListDirs {
    pub(crate) fn from_frame_multi(frame: Frame) -> Result<Vec<String>, TypedResponseError> {
        let mut out = Vec::with_capacity(frame.fields_len());

        for (key, value) in frame {
            if key.as_ref() == "directory" {
                out.push(value);
            }
        }

        Ok(out)
    }
}

/// Output from config
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Output {
    pub id: usize,
    pub name: String,
    pub plugin: String,
    pub enabled: bool,
}

impl Output {
    pub(crate) fn from_frame_multi(frame: Frame) -> Result<Vec<Output>, TypedResponseError> {
        let mut out = Vec::with_capacity(frame.fields_len());
        let mut builder = OutputBuilder::default();

        for (key, value) in frame {
            if let Some(output) = builder.field(&key, value)? {
                out.push(output);
            }
        }

        Ok(out)
    }
}

#[derive(Default)]
struct OutputBuilder {
    pub id: String,
    pub name: String,
    pub plugin: String,
    pub enabled: String,
}

impl OutputBuilder {
    /// Handle a field from a song list.
    ///
    /// If this returns `Ok(Some(_))`, an output was completed and another one started.
    fn field(&mut self, key: &str, value: String) -> Result<Option<Output>, TypedResponseError> {
        if self.id.is_empty() {
            // No output is currently in progress
            self.handle_start_field(key, value)?;
            Ok(None)
        } else {
            // Currently parsing a song
            self.handle_field(key, value)
        }
    }

    /// Handle a field that is expected to start a new song.
    fn handle_start_field(&mut self, key: &str, value: String) -> Result<(), TypedResponseError> {
        match key {
            // A `file` field starts a new song
            "outputid" => self.id = value,
            other => return Err(TypedResponseError::unexpected_field("outputid", other)),
        }

        Ok(())
    }

    /// Handle a field that may be part of a song or may start a new one.
    fn handle_field(
        &mut self,
        key: &str,
        value: String,
    ) -> Result<Option<Output>, TypedResponseError> {
        // If this field starts a new song, the current one is done
        if key == "outputid" {
            // Reset the song builder and convert the existing data into a song
            let song = std::mem::take(self).into_output();

            // Handle the current field
            self.handle_start_field(key, value)?;

            // Return the complete song
            return Ok(Some(song));
        }

        // The field is a component of a song
        match key {
            "outputid" => self.id = value,
            "outputname" => self.name = value,
            "plugin" => self.plugin = value,
            "outputenabled" => self.enabled = value,
            _ => (),
        }

        Ok(None)
    }

    /// Finish the building process. This returns the final song, if there is one.
    fn finish(self) -> Option<Output> {
        if self.id.is_empty() {
            None
        } else {
            Some(self.into_output())
        }
    }

    fn into_output(self) -> Output {
        assert!(!self.id.is_empty());

        Output {
            id: self.id.parse().unwrap(),
            name: self.name,
            plugin: self.plugin,
            enabled: matches!(self.enabled.as_str(), "1"),
        }
    }
}
