//! Message Post policy gate.
//!
//! The router remains pure lookup. This module runs after lookup and before `broadcast_messages`
//! so denied sends never create `messages` or `in_flight` rows. It is still the bus spine, not a UI
//! concern: every CLI/MCP/gateway Message Post path enters here through `BusPort::send`. Direct
//! messages are allowed across durable policy groups only when both agents are current co-members
//! of an active thread; that shared thread is already intentional context and removal/archive
//! revokes the DM allowance on the next send.

use nexus_common::NexusError;
use nexus_contracts::enums::Tier;
use nexus_contracts::ports::Caller;
use nexus_store::repos::{AgentGroups, Threads};
use nexus_store::Store;

use crate::router::{Recipient, Resolved};

/// Store-backed policy evaluator for one resolved Message Post send.
pub(crate) struct MessagePolicy<'a> {
    store: &'a Store,
}

impl<'a> MessagePolicy<'a> {
    /// Bind the policy evaluator to the same store the bus will write to.
    pub(crate) fn new(store: &'a Store) -> Self {
        MessagePolicy { store }
    }

    /// Check the resolved target before canonical Message Post rows are written.
    pub(crate) async fn check(
        &self,
        caller: &Caller,
        resolved: &Resolved,
    ) -> Result<(), NexusError> {
        match resolved {
            Resolved::Dm(recipient) => self.check_dm(caller, recipient).await,
            Resolved::LocalOperatorDm(_) => Ok(()),
            Resolved::Thread(thread_id, _) => {
                if caller.tier == Tier::Admin {
                    return Ok(());
                }
                if Threads::new(self.store)
                    .is_member(thread_id, &caller.name)
                    .await?
                {
                    Ok(())
                } else {
                    Err(policy_denied(format!(
                        "{} is not a member of thread {}",
                        caller.name, thread_id.0
                    )))
                }
            }
            Resolved::Topic(_, _) => Ok(()),
            Resolved::Group(_, _) => Ok(()),
        }
    }

    async fn check_dm(&self, caller: &Caller, recipient: &Recipient) -> Result<(), NexusError> {
        if caller.tier == Tier::Admin {
            return Ok(());
        }
        let Some(from_agent_id) = caller.agent_id.as_ref() else {
            return Ok(());
        };
        let Some(to_agent_id) = recipient.agent_id.as_ref() else {
            return Ok(());
        };
        if from_agent_id == to_agent_id {
            return Ok(());
        }

        let groups = AgentGroups::new(self.store);
        if groups
            .share_group(&caller.project, from_agent_id, to_agent_id)
            .await?
        {
            return Ok(());
        }
        if Threads::new(self.store)
            .share_active_thread_by_agent(&from_agent_id.0, &to_agent_id.0)
            .await?
        {
            return Ok(());
        }

        let from_groups = groups
            .groups_for_agent(&caller.project, from_agent_id)
            .await?;
        let to_groups = groups
            .groups_for_agent(&caller.project, to_agent_id)
            .await?;
        if from_groups.is_empty() && to_groups.is_empty() {
            return Ok(());
        }

        Err(policy_denied(format!(
            "{} and {} do not share a policy group",
            caller.name,
            recipient.name.as_deref().unwrap_or(&recipient.session.0)
        )))
    }
}

fn policy_denied(reason: String) -> NexusError {
    NexusError::PolicyDenied(reason)
}
