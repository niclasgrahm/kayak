//! When the canvas should offer to create a project.
//!
//! A server started without `--config` is a blank instance: nothing on the
//! canvas, no file a save would go to. The friendly thing to do with that
//! screen is to say so and offer the first step — a dialog that creates the
//! config file — rather than presenting an empty canvas whose "create config
//! file" button is two clicks deep in edit mode. This module is the pure half
//! of that decision; `app.rs` holds the dialog itself.

/// What is known about the instance once the two resources have answered.
///
/// Both fields are `None` until their request comes back (or when it failed),
/// and the distinction is load-bearing: the dialog must not flash up while the
/// answers are still in flight, so "don't know yet" has to be a state rather
/// than a default. The derived `config_file` signal in `app.rs` collapses
/// "loading" and "no file" into one `None`, which is right for the navbar's
/// buttons and wrong here — hence this type reads the resources itself.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Instance {
    /// `Some(inner)` once the settings arrived; `inner` is the config file's
    /// name, if the server has one.
    pub config_file: Option<Option<String>>,
    /// `Some(count)` once the pipeline list arrived.
    pub pipelines: Option<usize>,
}

impl Instance {
    /// A server with no config file *and* nothing running: the state the
    /// creator dialog exists for. A server with a file but an empty graph is
    /// not blank — its project exists, there is just nothing in it yet — and a
    /// server without a file but with pipelines is being driven by a script,
    /// which a welcome dialog would only interrupt.
    #[must_use]
    pub fn is_blank(&self) -> bool {
        self.config_file == Some(None) && self.pipelines == Some(0)
    }
}

/// Whether to show the creator dialog right now.
///
/// `may_edit` because creating a project is a save, and offering a reader a
/// dialog whose one button the server would 403 is worse than not offering it.
/// `dismissed` is the session's "not now" — the empty canvas behind the dialog
/// keeps a way back in, so declining is never a dead end.
#[must_use]
pub fn offer_creator(instance: &Instance, may_edit: bool, dismissed: bool) -> bool {
    may_edit && !dismissed && instance.is_blank()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blank() -> Instance {
        Instance {
            config_file: Some(None),
            pipelines: Some(0),
        }
    }

    #[test]
    fn a_blank_instance_is_offered_the_creator() {
        assert!(offer_creator(&blank(), true, false));
    }

    #[test]
    fn nothing_is_offered_while_the_answers_are_in_flight() {
        // dialogs that flash up and vanish during load train people to
        // ignore them — same reasoning as the "unsaved changes" warning
        assert!(!offer_creator(&Instance::default(), true, false));
        assert!(!offer_creator(
            &Instance {
                config_file: None,
                pipelines: Some(0),
            },
            true,
            false,
        ));
        assert!(!offer_creator(
            &Instance {
                config_file: Some(None),
                pipelines: None,
            },
            true,
            false,
        ));
    }

    #[test]
    fn a_server_with_a_config_file_is_not_blank() {
        let instance = Instance {
            config_file: Some(Some("config.json".to_string())),
            pipelines: Some(0),
        };
        assert!(!offer_creator(&instance, true, false));
    }

    #[test]
    fn a_server_with_pipelines_is_not_blank() {
        // no file but a running graph: something is already driving this
        // server over the API, and a welcome dialog would be in its way
        let instance = Instance {
            config_file: Some(None),
            pipelines: Some(2),
        };
        assert!(!offer_creator(&instance, true, false));
    }

    #[test]
    fn a_reader_is_not_offered_a_dialog_the_server_would_refuse() {
        assert!(!offer_creator(&blank(), false, false));
    }

    #[test]
    fn dismissing_the_dialog_sticks() {
        assert!(!offer_creator(&blank(), true, true));
    }
}
