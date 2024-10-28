// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::future::Future;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// A temporary authentication token with an associated expiry
#[derive(Debug, Clone)]
pub struct TemporaryToken<T> {
    /// The temporary credential
    pub token: T,
    /// The instant at which this credential is no longer valid
    /// None means the credential does not expire
    pub expiry: Option<Instant>,
}

/// Provides [`TokenCache::get_or_insert_with`] which can be used to cache a
/// [`TemporaryToken`] based on its expiry
#[derive(Debug)]
pub struct TokenCache<T> {
    cache: Mutex<Option<(TemporaryToken<T>, Instant)>>,
    min_ttl: Duration,
    fetch_backoff: Duration,
}

impl<T> Default for TokenCache<T> {
    fn default() -> Self {
        Self {
            cache: Default::default(),
            min_ttl: Duration::from_secs(300),
            // How long to wait before re-attempting a token fetch after receiving one that
            // is still within the min-ttl
            fetch_backoff: Duration::from_millis(100),
        }
    }
}

impl<T: Clone + Send> TokenCache<T> {
    /// Override the minimum remaining TTL for a cached token to be used
    #[cfg(any(feature = "aws", feature = "gcp"))]
    pub fn with_min_ttl(self, min_ttl: Duration) -> Self {
        Self { min_ttl, ..self }
    }

    pub async fn get_or_insert_with<F, Fut, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<TemporaryToken<T>, E>> + Send,
    {
        let now = Instant::now();
        let mut locked = self.cache.lock().await;

        if let Some((cached, fetched_at)) = locked.as_ref() {
            match cached.expiry {
                Some(ttl) => {
                    if ttl.checked_duration_since(now).unwrap_or_default() > self.min_ttl ||
                        // if we've recently attempted to fetch this token and it's not actually
                        // expired, we'll wait to re-fetch it and return the cached one
                        (fetched_at.elapsed() < self.fetch_backoff && ttl.checked_duration_since(now).is_some())
                    {
                        return Ok(cached.token.clone());
                    }
                }
                None => return Ok(cached.token.clone()),
            }
        }

        let cached = f().await?;
        let token = cached.token.clone();
        *locked = Some((cached, Instant::now()));

        Ok(token)
    }
}
