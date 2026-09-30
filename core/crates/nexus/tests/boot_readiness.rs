// Exercise the exact private production orchestration, without public injection APIs.
mod boot_readiness {
    include!("../src/daemon/boot_readiness.rs");

    mod tests {
        use super::*;
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        use std::time::Duration;
        use tokio::sync::oneshot;

        fn guard(readiness: &BootReadiness) -> (BootGuard, Arc<AtomicUsize>) {
            let closed = Arc::new(AtomicUsize::new(0));
            let capture = closed.clone();
            (
                readiness.guard(move || {
                    capture.fetch_add(1, Ordering::SeqCst);
                }),
                closed,
            )
        }

        async fn ingress(readiness: &BootReadiness) -> Result<(), NexusError> {
            tokio::time::timeout(Duration::from_secs(2), readiness.wait_ingress())
                .await
                .expect("ingress waiter stranded")
        }

        async fn presence(readiness: &BootReadiness) -> Result<InitialPresenceOutcome, NexusError> {
            tokio::time::timeout(Duration::from_secs(2), readiness.wait_initial_presence())
                .await
                .expect("presence waiter stranded")
        }

        #[tokio::test]
        async fn production_phases_gate_ingress_and_cadence_independently() {
            let readiness = BootReadiness::pending();
            let (guard, closed) = guard(&readiness);
            let (model_entered, model_started) = oneshot::channel();
            let (model_release, model_gate) = oneshot::channel();
            let (directory_entered, directory_started) = oneshot::channel();
            let (directory_release, directory_gate) = oneshot::channel();
            let (adoption_entered, adoption_started) = oneshot::channel();
            let (adoption_release, adoption_gate) = oneshot::channel();
            let (backlog_entered, backlog_started) = oneshot::channel();
            let (backlog_release, backlog_gate) = oneshot::channel();
            let task = tokio::spawn(run_boot(
                guard,
                async {
                    model_entered.send(()).unwrap();
                    model_gate.await.unwrap();
                    Ok(())
                },
                async {
                    directory_entered.send(()).unwrap();
                    directory_gate.await.unwrap();
                    Ok(())
                },
                async {
                    adoption_entered.send(()).unwrap();
                    adoption_gate.await.unwrap();
                    Ok(InitialPresenceOutcome {
                        presume_dead: Err("best effort presume-dead failure".into()),
                        stale: Err("best effort stale failure".into()),
                    })
                },
                async {
                    backlog_entered.send(()).unwrap();
                    backlog_gate.await.unwrap();
                },
            ));
            model_started.await.unwrap();
            let mut due_tick = tokio::time::interval(Duration::from_secs(30));
            due_tick.tick().await; // Tokio's first cadence tick is already due, even during init.
            let mut directory_started = Box::pin(directory_started);
            assert!(futures::poll!(&mut directory_started).is_pending());
            let mut ingress_wait = Box::pin(readiness.wait_ingress());
            let mut presence_wait = Box::pin(readiness.wait_initial_presence());
            assert!(futures::poll!(&mut ingress_wait).is_pending());
            assert!(futures::poll!(&mut presence_wait).is_pending());
            model_release.send(()).unwrap();
            directory_started.await.unwrap();
            assert!(futures::poll!(&mut ingress_wait).is_pending());
            assert!(futures::poll!(&mut presence_wait).is_pending());
            directory_release.send(()).unwrap();
            adoption_started.await.unwrap();
            due_tick.reset_immediately();
            due_tick.tick().await;
            ingress_wait.await.unwrap();
            ingress(&readiness).await.unwrap();
            assert!(
                futures::poll!(&mut presence_wait).is_pending(),
                "elapsed cadence must not bypass adoption"
            );
            adoption_release.send(()).unwrap();
            backlog_started.await.unwrap();
            let completed = presence_wait.await.unwrap();
            assert_eq!(
                completed.presume_dead,
                Err("best effort presume-dead failure".into())
            );
            assert_eq!(completed.stale, Err("best effort stale failure".into()));
            assert_eq!(presence(&readiness).await.unwrap(), completed);
            backlog_release.send(()).unwrap();
            task.await.unwrap().unwrap();
            assert_eq!(
                closed.load(Ordering::SeqCst),
                0,
                "normal completion must disarm guard"
            );
        }

        #[tokio::test]
        async fn model_failure_settles_current_and_later_waiters_without_running_directory() {
            let readiness = BootReadiness::pending();
            let (guard, closed) = guard(&readiness);
            let mut current = Box::pin(readiness.wait_ingress());
            let mut cadence = Box::pin(readiness.wait_initial_presence());
            assert!(futures::poll!(&mut current).is_pending());
            assert!(futures::poll!(&mut cadence).is_pending());
            let result = run_boot(
                guard,
                async { Err(internal("model authority corrupt")) },
                async { panic!("directory must not run") },
                async { panic!("adoption must not run") },
                async { panic!("backlog must not run") },
            )
            .await;
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("model authority corrupt"));
            assert!(current
                .await
                .unwrap_err()
                .to_string()
                .contains("model authority corrupt"));
            assert!(cadence.await.is_err());
            assert!(ingress(&readiness).await.is_err());
            assert!(presence(&readiness).await.is_err());
            assert_eq!(closed.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn fatal_pre_adoption_exit_preserves_ingress_success_but_rejects_cadence() {
            let readiness = BootReadiness::pending();
            let (guard, closed) = guard(&readiness);
            run_boot(
                guard,
                async { Ok(()) },
                async { Ok(()) },
                async { Err(internal("delivery restoration failed")) },
                async { panic!("no backlog") },
            )
            .await
            .unwrap_err();
            ingress(&readiness).await.unwrap();
            assert!(presence(&readiness)
                .await
                .unwrap_err()
                .to_string()
                .contains("delivery restoration failed"));
            assert_eq!(closed.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn directory_failure_retains_successful_model_outcome() {
            let readiness = BootReadiness::pending();
            let (guard, closed) = guard(&readiness);
            run_boot(
                guard,
                async { Ok(()) },
                async { Err(internal("directory insert failed")) },
                async { panic!("no adoption") },
                async {},
            )
            .await
            .unwrap_err();
            assert_eq!(readiness.outcomes.borrow().model, Some(Ok(())));
            assert!(ingress(&readiness)
                .await
                .unwrap_err()
                .to_string()
                .contains("directory insert failed"));
            assert!(presence(&readiness).await.is_err());
            assert_eq!(closed.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn pre_first_poll_abort_settles_both_waiters() {
            let readiness = BootReadiness::pending();
            let (guard, closed) = guard(&readiness);
            let mut current = Box::pin(readiness.wait_ingress());
            let mut cadence = Box::pin(readiness.wait_initial_presence());
            assert!(futures::poll!(&mut current).is_pending());
            assert!(futures::poll!(&mut cadence).is_pending());
            let task = tokio::spawn(run_boot(
                guard,
                async { panic!("must never be polled") },
                async { Ok(()) },
                async { Ok(InitialPresenceOutcome::default()) },
                async {},
            ));
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert!(current.await.is_err());
            assert!(cadence.await.is_err());
            assert!(ingress(&readiness).await.is_err());
            assert!(presence(&readiness).await.is_err());
            assert_eq!(closed.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn panic_and_inflight_abort_settle_waiters_without_rewriting_completed_phases() {
            for panic in [false, true] {
                let readiness = BootReadiness::pending();
                let (guard, closed) = guard(&readiness);
                let (entered, started) = oneshot::channel();
                let (release, gate) = oneshot::channel();
                let task = tokio::spawn(run_boot(
                    guard,
                    async { Ok(()) },
                    async { Ok(()) },
                    async move {
                        entered.send(()).unwrap();
                        gate.await.unwrap();
                        panic!("adoption panic");
                    },
                    async {},
                ));
                started.await.unwrap();
                let mut waiter = Box::pin(readiness.wait_initial_presence());
                assert!(futures::poll!(&mut waiter).is_pending());
                if panic {
                    release.send(()).unwrap();
                } else {
                    task.abort();
                }
                assert!(task.await.is_err());
                assert!(waiter.await.is_err());
                assert!(presence(&readiness).await.is_err());
                ingress(&readiness).await.unwrap();
                assert_eq!(closed.load(Ordering::SeqCst), 1);
            }
        }

        #[tokio::test]
        async fn backlog_panic_keeps_completed_presence_and_ingress_but_closes_admission() {
            let readiness = BootReadiness::pending();
            let (guard, closed) = guard(&readiness);
            let task = tokio::spawn(run_boot(
                guard,
                async { Ok(()) },
                async { Ok(()) },
                async { Ok(InitialPresenceOutcome::default()) },
                async { panic!("backlog panic") },
            ));
            assert!(task.await.unwrap_err().is_panic());
            ingress(&readiness).await.unwrap();
            presence(&readiness).await.unwrap();
            assert_eq!(closed.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn unmanaged_readiness_does_not_pretend_model_initialization_ran() {
            let readiness = BootReadiness::unmanaged();
            ingress(&readiness).await.unwrap();
            assert!(readiness.outcomes.borrow().model.is_none());
        }
    }
}
