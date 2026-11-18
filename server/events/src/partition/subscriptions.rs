use serde::{Deserialize, Serialize};

use super::{
    Command, Error, ErrorCode, HEAD_KEY, Head, Lease, MAX_LEASE_MS,
    MAX_SUBSCRIPTIONS_PER_PARTITION, MAX_TIMESTAMP_MS, Query, QueryReply, Reply, Start, Step,
    SubscriptionStatus, Transaction, Uuid, events, load, store, validate_time,
};

const SUBSCRIPTION_PREFIX: &[u8] = b"partition/v1/subscription/";

#[derive(Serialize, Deserialize)]
struct Subscription {
    start: Start,
    checkpoint: u64,
    lease: Option<StoredLease>,
}

#[derive(Serialize, Deserialize)]
struct StoredLease {
    owner: String,
    claim_id: Uuid,
    generation: u64,
    expires_ms: i64,
    released: bool,
}

impl StoredLease {
    fn has_scope(&self, owner: &str, generation: u64, now_ms: i64) -> bool {
        self.owner == owner && self.generation == generation && now_ms < self.expires_ms
    }

    fn receipt(&self) -> Lease {
        Lease {
            generation: self.generation,
            expires_ms: self.expires_ms,
        }
    }
}

pub(super) fn apply(tx: &mut Transaction<'_>, head: &mut Head, command: Command) -> Step<Reply> {
    match command {
        Command::OpenSubscription { name, start } => open(tx, head, &name, start),
        Command::ResetExpired { name } => reset_expired(tx, head, &name),
        Command::Claim {
            name,
            owner,
            claim_id,
            now_ms,
            ttl_ms,
        } => change_lease(tx, head, &name, &owner, now_ms, |_, sub, now_ms| {
            sub.claim(&owner, claim_id, now_ms, ttl_ms)
                .map(Reply::Lease)
        }),
        Command::Renew {
            name,
            owner,
            generation,
            now_ms,
            ttl_ms,
        } => change_lease(tx, head, &name, &owner, now_ms, |_, sub, now_ms| {
            sub.renew(&owner, generation, now_ms, ttl_ms)
                .map(Reply::Lease)
        }),
        Command::Release {
            name,
            owner,
            generation,
            now_ms,
        } => change_lease(tx, head, &name, &owner, now_ms, |_, sub, now_ms| {
            sub.release(&owner, generation, now_ms)?;
            Ok(Reply::Done)
        }),
        Command::Ack {
            name,
            owner,
            generation,
            after,
            last,
            through,
            now_ms,
        } => change_lease(tx, head, &name, &owner, now_ms, |head, sub, now_ms| {
            sub.live_lease(&owner, generation, now_ms)?;
            if after >= through || through > last || last > head.tail() || after > sub.checkpoint {
                return Err(Error::new(
                    ErrorCode::InvalidRequest,
                    "acknowledgment is outside the delivered prefix",
                ));
            }
            if through > sub.checkpoint {
                if sub.checkpoint < head.first_checkpoint() {
                    return Err(head.expired());
                }
                sub.checkpoint = through;
            }
            Ok(Reply::Checkpoint {
                offset: sub.checkpoint,
            })
        }),
        _ => unreachable!(),
    }
}

pub(super) fn read(tx: &Transaction<'_>, head: &Head, query: Query) -> Step<QueryReply> {
    match query {
        Query::Poll {
            name,
            owner,
            generation,
            now_ms,
            limit,
        } => {
            validate_owner(&owner)?;
            validate_time(now_ms)?;
            let sub = get(tx, &subscription_key(&name)?)?;
            sub.live_lease(&owner, generation, now_ms.max(head.lease_clock_ms))?;
            Ok(QueryReply::Poll(events::page(
                tx,
                head,
                sub.checkpoint,
                limit,
            )?))
        }
        Query::SubscriptionStatus { name } => {
            let sub = get(tx, &subscription_key(&name)?)?;
            let lease = sub.lease.filter(|lease| !lease.released);
            Ok(QueryReply::SubscriptionStatus(SubscriptionStatus {
                checkpoint: sub.checkpoint,
                earliest_offset: head.earliest_offset,
                tail: head.tail(),
                lease_generation: lease.as_ref().map(|lease| lease.generation),
                lease_expires_ms: lease.map(|lease| lease.expires_ms),
            }))
        }
        Query::Status => unreachable!(),
    }
}

impl Subscription {
    fn claim(
        &mut self,
        owner: &str,
        claim_id: Uuid,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<Lease, Error> {
        let expires_ms = expiry(now_ms, ttl_ms)?;
        let generation = match &self.lease {
            Some(lease) => {
                if lease.owner == owner && lease.claim_id == claim_id {
                    return if !lease.released && now_ms < lease.expires_ms {
                        Ok(lease.receipt())
                    } else {
                        Err(lease_lost())
                    };
                }
                if !lease.released && now_ms < lease.expires_ms {
                    return Err(Error::new(
                        ErrorCode::LeaseBusy,
                        "another grant already holds this partition",
                    ));
                }
                lease.generation.checked_add(1).ok_or_else(|| {
                    Error::new(
                        ErrorCode::Conflict,
                        "subscription has exhausted its generations",
                    )
                })?
            }
            None => 1,
        };

        let lease = StoredLease {
            owner: owner.into(),
            claim_id,
            generation,
            expires_ms,
            released: false,
        };
        let receipt = lease.receipt();
        self.lease = Some(lease);
        Ok(receipt)
    }

    fn renew(
        &mut self,
        owner: &str,
        generation: u64,
        now_ms: i64,
        ttl_ms: i64,
    ) -> Result<Lease, Error> {
        let expires_ms = expiry(now_ms, ttl_ms)?;
        let lease = self
            .lease
            .as_mut()
            .filter(|lease| !lease.released && lease.has_scope(owner, generation, now_ms))
            .ok_or_else(lease_lost)?;
        lease.expires_ms = lease.expires_ms.max(expires_ms);
        Ok(lease.receipt())
    }

    fn release(&mut self, owner: &str, generation: u64, now_ms: i64) -> Result<(), Error> {
        let lease = self
            .lease
            .as_mut()
            .filter(|lease| lease.has_scope(owner, generation, now_ms))
            .ok_or_else(lease_lost)?;
        lease.released = true;
        Ok(())
    }

    fn live_lease(&self, owner: &str, generation: u64, now_ms: i64) -> Result<&StoredLease, Error> {
        self.lease
            .as_ref()
            .filter(|lease| !lease.released && lease.has_scope(owner, generation, now_ms))
            .ok_or_else(lease_lost)
    }
}

fn open(tx: &mut Transaction<'_>, head: &mut Head, name: &str, start: Start) -> Step<Reply> {
    let key = subscription_key(name)?;
    if let Some(sub) = load::<Subscription>(tx, &key)? {
        if sub.start != start {
            return Err(
                Error::new(ErrorCode::Conflict, "subscription start policy differs").into(),
            );
        }
        return Ok(Reply::Done);
    }
    if head.subscription_count >= MAX_SUBSCRIPTIONS_PER_PARTITION {
        return Err(Error::new(ErrorCode::Conflict, "partition has too many subscriptions").into());
    }
    let checkpoint = match start {
        Start::Earliest => head.first_checkpoint(),
        Start::Latest => head.tail(),
    };
    store(
        tx,
        &key,
        &Subscription {
            start,
            checkpoint,
            lease: None,
        },
    )?;
    head.subscription_count += 1;
    store(tx, HEAD_KEY, head)?;
    Ok(Reply::Done)
}

fn reset_expired(tx: &mut Transaction<'_>, head: &Head, name: &str) -> Step<Reply> {
    let key = subscription_key(name)?;
    let mut sub = get(tx, &key)?;
    if sub.checkpoint < head.first_checkpoint() {
        sub.checkpoint = head.first_checkpoint();
        if let Some(lease) = &mut sub.lease {
            lease.released = true;
        }
        store(tx, &key, &sub)?;
    }
    Ok(Reply::Checkpoint {
        offset: sub.checkpoint,
    })
}

fn change_lease(
    tx: &mut Transaction<'_>,
    head: &mut Head,
    name: &str,
    owner: &str,
    now_ms: i64,
    edit: impl FnOnce(&Head, &mut Subscription, i64) -> Result<Reply, Error>,
) -> Step<Reply> {
    validate_owner(owner)?;
    validate_time(now_ms)?;
    let key = subscription_key(name)?;
    let mut sub = get(tx, &key)?;
    if now_ms > head.lease_clock_ms {
        head.lease_clock_ms = now_ms;
        // Keep the clock even when edit returns a domain rejection. A later leader
        // with a slower clock cannot revive a grant that an applied command saw expire.
        store(tx, HEAD_KEY, head)?;
    }
    let reply = edit(head, &mut sub, head.lease_clock_ms)?;
    store(tx, &key, &sub)?;
    Ok(reply)
}

fn expiry(now_ms: i64, ttl_ms: i64) -> Result<i64, Error> {
    if !(1..=MAX_LEASE_MS).contains(&ttl_ms) || now_ms > MAX_TIMESTAMP_MS - ttl_ms {
        return Err(Error::new(
            ErrorCode::InvalidRequest,
            "invalid lease lifetime",
        ));
    }
    Ok(now_ms + ttl_ms)
}

fn validate_owner(owner: &str) -> Result<(), Error> {
    if owner.is_empty() || owner.len() > 256 {
        return Err(Error::new(ErrorCode::InvalidRequest, "invalid lease owner"));
    }
    Ok(())
}

fn lease_lost() -> Error {
    Error::new(ErrorCode::LeaseLost, "lease is expired or no longer held")
}

fn get(tx: &Transaction<'_>, key: &[u8]) -> Step<Subscription> {
    load(tx, key)?
        .ok_or_else(|| Error::new(ErrorCode::NotFound, "subscription does not exist").into())
}

fn subscription_key(name: &str) -> Result<Vec<u8>, Error> {
    if !super::valid_name(name) {
        return Err(Error::new(
            ErrorCode::InvalidRequest,
            "invalid subscription name",
        ));
    }
    Ok([SUBSCRIPTION_PREFIX, name.as_bytes()].concat())
}
