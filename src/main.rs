use std::fmt::Debug;
use stateright::{Expectation, Model, Checker};
use stateright::actor::{
    Actor, ActorModel, DuplicatingNetwork, Id, model_peers, Out};
use std::ops::Range;
use std::time::Duration;
use std::borrow::Cow;
use HeartbeatMsg::*;

pub mod paxos;

/// An enum for all the different BLE message types.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum HeartbeatMsg {
    Request { round: u32, ballot_max: Ballot },
    Reply { round: u32, ballot: Ballot },
}

/// Used to define an epoch
#[derive(Clone, Copy, Eq, Debug, Default, Ord, PartialOrd, PartialEq, Hash)]
pub struct Ballot {
    /// Ballot number
    pub n: u32,
    /// The pid of the process
    pub pid: Id,
}

impl Ballot {
    /// Creates a new Ballot
    /// # Arguments
    /// * `n` - Ballot number.
    /// * `pid` -  Used as tiebreaker for total ordering of ballots.
    pub fn with(n: u32, pid: Id) -> Ballot {
        Ballot { n, pid }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BallotLeaderElectionState {
    /// The current round of the heartbeat cycle.
    pub hb_round: u32,
    /// Vector which holds all the received ballots.
    pub ballots: Vec<Ballot>,
    /// Holds the current ballot of this instance.
    pub current_ballot: Ballot, // (round, pid)
    /// Max ballot
    pub ballot_max: Ballot,
    /// States if the instance is a candidate to become a leader.
    pub leader: Option<Ballot>,
    /// Hold all the leaders that have been elected.
    pub leader_history: Vec<Ballot>,
    /// How long time is waited before timing out on a Heartbeat response and possibly resulting in a leader-change.
    pub hb_delay: Range<Duration>,
    /// Delta delay
    pub increment_delay: Duration,
}

/// A Ballot Leader Election component. Used in conjunction with Omni-Paxos handles the election of a leader for a group of omni-paxos replicas,
/// incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
/// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
/// #[derive(Clone)]
pub struct BallotLeaderElectionActor {
    /// Process identifier used to uniquely identify this instance.
    pub pid: Id,
    /// Vector that holds all the other replicas.
    pub peers: Vec<Id>,
    /// The majority of replicas inside a cluster. It is measured in ticks.
    pub majority: usize,

    pub max_round: u32
}

impl BallotLeaderElectionActor {
    /// Construct a new BallotLeaderComponent
    /// # Arguments
    /// * `peers` - Vector that holds all the other replicas.
    /// * `pid` -  Process identifier used to uniquely identify this instance.
    pub fn with(
        peers: Vec<Id>,
        pid: Id,
        max_round: u32
    ) -> BallotLeaderElectionActor {
        let n = &peers.len() + 1;

        BallotLeaderElectionActor {
            pid,
            peers,
            majority: n / 2 + 1, // +1 because peers is exclusive ourselves
            max_round
        }
    }
}

impl Actor for BallotLeaderElectionActor {
    type Msg = HeartbeatMsg;
    type State = BallotLeaderElectionState;

    fn on_start(&self, id: Id, o: &mut Out<Self>) -> Self::State {
        let delay = Duration::from_millis(500)..Duration::from_millis(500);
        o.set_timer(delay.clone());

        BallotLeaderElectionState {
            hb_round: 0,
            ballots: Vec::new(),
            current_ballot: Ballot::with(0, id),
            ballot_max: Default::default(),
            leader: None,
            leader_history: Vec::new(),
            hb_delay: delay,
            increment_delay: Duration::from_millis(200)
        }
    }

    fn on_msg(&self, _id: Id, state: &mut Cow<Self::State>, src: Id, msg: Self::Msg,o : &mut Out<Self>) {
        let mut state = state.to_mut();
        match msg {
            Request { round, ballot_max } => {
                if ballot_max > state.ballot_max {
                    state.ballot_max = ballot_max;
                }
                o.send(src, Reply{ round, ballot: state.current_ballot });
            }
            Reply { round, ballot } => {
                if round == state.hb_round {
                    state.ballots.push(ballot);
                } else {
                    state.hb_delay = (state.hb_delay.start + state.increment_delay)..(state.hb_delay.end + state.increment_delay);
                }
            }
        }
    }

    fn on_timeout(&self, _id: Id, state: &mut Cow<Self::State>, o: &mut Out<Self>) {
        let mut state = state.to_mut();
        let mut bal = state.ballots.clone();
        if bal.len() + 1 >= self.majority {
            bal.push(state.current_ballot);
            let top = bal.into_iter().max().unwrap_or_default();

            if top < state.ballot_max {
                loop {
                    if state.current_ballot > state.ballot_max {
                        break
                    }

                    state.current_ballot.n = state.current_ballot.n + 1;
                    state.leader = None;
                }
            } else if (state.leader.is_some() && top != state.leader.unwrap()) || state.leader.is_none() {
                state.ballot_max = top;
                state.leader = Some(top);

                state.leader_history.push(state.leader.unwrap());
                // trigger leader
            }
        }

        state.ballots.drain(..);
        state.hb_round += 1;

        if state.hb_round > self.max_round {
            return;
        }

        for peer in self.peers.iter() {
            let p = peer.clone();
            if p != self.pid {
                o.send(p, Request{ round: state.hb_round, ballot_max: state.ballot_max });
            }
        }

        o.set_timer(state.hb_delay.clone());
    }
}

#[derive(Clone)]
struct BLEModelCfg {
    server_count: usize,
}

impl BLEModelCfg {
    fn into_model(self) ->
    ActorModel<
        BallotLeaderElectionActor,
        Self
        >
    {
        ActorModel::new(self.clone(), ())
            .actors((0..self.server_count)
                .map(|i| BallotLeaderElectionActor {
                    pid: Id::from(i),
                    peers: model_peers(i, self.server_count),
                    majority: 0,
                    max_round: 3
                }))
            .duplicating_network(DuplicatingNetwork::No)
            .property(Expectation::Eventually, "eventual agreement", |_, state| {
                let mut l = None;
                for env in &state.actor_states {
                    if l == None {
                        l = env.leader;
                    } else if l != env.leader {
                        return false;
                    }
                }

                return true;
            })
            .property(Expectation::Always, "monotonic unique ballots", |_, state| {
                let mut sign = true;
                for env in &state.actor_states {
                    sign = sign && is_sorted(env.leader_history.as_ref());
                }

                return sign;
            })
    }
}

fn is_sorted<T>(data: &Vec<T>) -> bool
    where
        T: Ord,
{
    data.windows(2).all(|w| w[0] <= w[1])
}

#[cfg(test)]
#[test]
fn can_model_ble() {
    use stateright::actor::ActorModelAction::{Deliver, Timeout};

    // BFS
    let checker = BLEModelCfg {
        server_count: 3,
    }
        .into_model().checker().spawn_bfs().join();
    checker.assert_properties();
    checker.assert_discovery("eventual agreement", vec![

    ]);
    assert_eq!(checker.unique_state_count(), 1);
}

fn main() -> Result<(), pico_args::Error> {
    env_logger::init_from_env(env_logger::Env::default()
        .default_filter_or("info")); // `RUST_LOG=${LEVEL}` env variable to override

    let mut args = pico_args::Arguments::from_env();
    match args.subcommand()?.as_deref() {
        Some("check") => {
            let server_count = args.opt_free_from_str()?
                .unwrap_or(7);
            println!("Model checking Ballot Leader Election with {} servers.",
                     server_count);
            BLEModelCfg {
                server_count,
            }
                .into_model().checker().threads(num_cpus::get())
                .spawn_dfs().report(&mut std::io::stdout());
        }
        Some("explore") => {
            let server_count = args.opt_free_from_str()?
                .unwrap_or(3);
            let address = args.opt_free_from_str()?
                .unwrap_or("localhost:3000".to_string());
            println!(
                "Exploring state space for Ballot Leader Election with {} servers on {}.",
                server_count, address);
            BLEModelCfg {
                server_count,
            }
                .into_model().checker().threads(num_cpus::get())
                .serve(address);
        }
        _ => {
            println!("USAGE:");
            println!("  ./ble check [SERVER_COUNT]");
            println!("  ./ble explore [SERVER_COUNT] [ADDRESS]");
        }
    }

    Ok(())
}


