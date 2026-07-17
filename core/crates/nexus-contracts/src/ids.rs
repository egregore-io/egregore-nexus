//! String-backed id newtypes. All are `#[serde(transparent)]` so they serialize
//! as a bare JSON string and map to `string` in the generated TypeScript.
//!
//! `AgentId` is the durable identity key. `SessionId` remains the runtime key.

use serde::{Deserialize, Serialize};
use typeshare::typeshare;

macro_rules! string_id {
    ($name:ident) => {
        #[typeshare]
        #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
        #[serde(transparent)]
        pub struct $name(pub String);

        impl From<String> for $name {
            fn from(s: String) -> Self {
                $name(s)
            }
        }
        impl From<&str> for $name {
            fn from(s: &str) -> Self {
                $name(s.to_string())
            }
        }
        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

string_id!(SessionId);
string_id!(AgentId);
string_id!(CredentialId);
string_id!(MessageId);
string_id!(ThreadId);
string_id!(TopicId);
string_id!(ProjectId);
