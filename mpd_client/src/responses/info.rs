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
