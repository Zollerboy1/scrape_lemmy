use std::{
    collections::HashMap, net::ToSocketAddrs as _, num::NonZeroU32, sync::Arc, time::Duration,
};

use async_once_cell::Lazy;
use async_recursion::async_recursion;
use async_trait::async_trait;
use futures::{
    future::{self, LocalBoxFuture},
    stream, FutureExt as _, StreamExt as _, TryStreamExt as _,
};
use governor::{Jitter, Quota, RateLimiter};
use lemmy_api_common::{
    comment::{GetComments, GetCommentsResponse},
    community::{GetCommunity, GetCommunityResponse, ListCommunities, ListCommunitiesResponse},
    lemmy_db_schema::{newtypes::InstanceId, CommentSortType, ListingType, SortType},
    lemmy_db_views::structs::SiteView,
    lemmy_db_views_actor::structs::CommunityView,
    person::{GetPersonDetails, GetPersonDetailsResponse, Login, LoginResponse},
    post::{GetPosts, GetPostsResponse},
    sensitive::Sensitive,
    site::{GetFederatedInstances, GetFederatedInstancesResponse, GetSite, GetSiteResponse},
};
use reqwest::{Client, Error as ReqwestError, Method, Request, Response, Url};
use serde::{de::DeserializeOwned, Serialize as _};
use tokio::sync::{mpsc, oneshot, RwLock};

use crate::{
    cli::Cli,
    error::{Error, LemmyError, Result},
    ext::{HasInner as _, IntoStream as _, StreamExt as _, TryStreamExt as _},
    lemmy::{
        LemmyComment, LemmyCommunityId, LemmyInstanceInfo, LemmyPost, LemmyPostId, LemmyUser,
        LemmyUserId,
    },
    log, path,
    path::{AuthParams, Params as _, Path},
};

path!(GetSitePath, "site", GetSite(Auth), GetSiteResponse);
path!(
    GetInstancesPath,
    "federated_instances",
    GetFederatedInstances(Auth),
    GetFederatedInstancesResponse
);
path!(
    GetCommunityPath,
    "community",
    GetCommunity,
    GetCommunityResponse
);
path!(
    ListCommunitiesPath,
    "community/list",
    ListCommunities,
    ListCommunitiesResponse
);
path!(ListPostsPath, "post/list", GetPosts, GetPostsResponse);
path!(
    ListCommentsPath,
    "comment/list",
    GetComments,
    GetCommentsResponse
);
path!(
    GetUserPath,
    "user",
    GetPersonDetails,
    GetPersonDetailsResponse
);


const USER_AGENT: &str = concat!(
    env!("CARGO_PKG_NAME"),
    "/",
    env!("CARGO_PKG_VERSION_MAJOR"),
    ".",
    env!("CARGO_PKG_VERSION_MINOR")
);

pub struct LemmyClient {
    cli: Arc<Cli>,
    domain: Arc<str>,
    tx: mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response, ReqwestError>>)>,
    retry_lock: Arc<RwLock<()>>,
    jwt: Option<Sensitive<String>>,
}

impl LemmyClient {
    fn build_reqwest_client(domain: &str) -> Result<Client> {
        let sock_addrs = format!("{}:443", domain).to_socket_addrs()?.collect::<Vec<_>>();
        Client::builder()
            .resolve_to_addrs(domain, &sock_addrs)
            .user_agent(USER_AGENT)
            .build()
            .map_err(Into::into)
    }

    async fn new(
        cli: Arc<Cli>,
        domain: Arc<str>,
        site_view: &SiteView,
        client: Client,
        jwt: Option<Sensitive<String>>,
    ) -> Result<LemmyClient> {
        let maximum_cells = site_view.local_site_rate_limit.message;
        let secs_to_refill = site_view.local_site_rate_limit.message_per_second;

        log!(
            "[{}] Message rate limit: {}/{}",
            domain,
            maximum_cells,
            secs_to_refill
        );

        let Some(quota) = (try {
            let maximum_cells = (maximum_cells / 6 + 1) as u32; // Let's be conservative here
            let max_burst = NonZeroU32::new(maximum_cells)?;

            let period = Duration::from_secs(secs_to_refill as u64) / maximum_cells;

            Quota::with_period(period)?.allow_burst(max_burst)
        }) else {
            do yeet Error::InvalidRateLimit;
        };

        let rate_limiter = Arc::new(RateLimiter::direct(quota));

        let (tx, rx) =
            mpsc::unbounded_channel::<(Request, oneshot::Sender<Result<Response, ReqwestError>>)>();

        let retry_lock = Arc::new(RwLock::new(()));

        {
            let retry_lock = retry_lock.clone();

            tokio::spawn(async move {
                let client = client;
                let mut rx = rx;

                let jitter = Jitter::up_to(Duration::from_secs(2));

                while let Some((req, tx)) = rx.recv().await {
                    rate_limiter.until_ready_with_jitter(jitter).await;

                    let client = client.clone();
                    let rate_limiter = rate_limiter.clone();
                    let retry_lock = retry_lock.clone();

                    tokio::spawn(async move {
                        if retry_lock.try_read().is_err() {
                            _ = retry_lock.read().await;
                            rate_limiter.until_ready_with_jitter(jitter).await;
                        }

                        _ = tx.send(client.execute(req).await);
                    });
                }
            });
        }

        Ok(LemmyClient {
            cli,
            domain,
            tx,
            retry_lock,
            jwt,
        })
    }

    pub async fn get<P>(&self, path: P) -> Result<P::Response>
    where
        P: Path,
        P::Params: AuthParams,
    {
        self.get_with_params(path, P::Params::new()).await
    }

    pub async fn get_with_params<P>(&self, path: P, params: P::Params) -> Result<P::Response>
    where
        P: Path,
    {
        let params = params.with_auth(self.jwt.clone());
        let url = self.url_with_params(path, params)?;

        self.get_impl(url, 0).await
    }

    #[async_recursion(?Send)]
    async fn get_impl<R>(&self, url: Url, n_retries: u8) -> Result<R>
    where
        R: DeserializeOwned,
    {
        let req = Request::new(Method::GET, url.clone());

        let (otx, orx) = oneshot::channel();

        self.tx.send((req, otx)).map_err(|_| Error::SendFailed)?;

        let res = orx.await.map_err(|_| Error::RecvFailed)?;

        let bytes: Result<_, ReqwestError> = try { res?.bytes().await? };

        let bytes = match bytes {
            Ok(bytes) => bytes,
            Err(e) => {
                log!("[{}] Reqwest error: {}", self.domain, e);

                return self.retry(url, n_retries).await;
            }
        };

        if let Ok(lemmy_error) = serde_json::from_slice::<LemmyError>(&bytes) {
            Err(Error::from(lemmy_error))
        } else if let Ok(res) = serde_json::from_slice::<R>(&bytes) {
            Ok(res)
        } else if bytes == "{}".as_bytes() {
            log!("[{}] Rate limit hit: {}", self.domain, url.as_str());
            self.retry(url, n_retries).await
        } else {
            log!(
                "[{}] Invalid response: {}",
                self.domain,
                String::from_utf8_lossy(&bytes)
            );

            self.retry(url, n_retries).await
        }
    }

    async fn retry<R>(&self, url: Url, n_retries: u8) -> Result<R>
    where
        R: DeserializeOwned,
    {
        if n_retries < self.cli.max_retries {
            let delay = Duration::from_secs(2u64.pow((n_retries + 2) as u32));
            log!("[{}] Trying again in {:?}", self.domain, delay);

            let write_guard = self.retry_lock.write().await;
            tokio::time::sleep(delay).await;
            std::mem::drop(write_guard);

            self.get_impl(url, n_retries + 1).await
        } else {
            Err(Error::TooManyRetries)
        }
    }

    fn url_with_params<P>(&self, path: P, params: P::Params) -> Result<Url>
    where
        P: Path,
    {
        let url_string = format!("https://{}/api/v3/{}", self.domain, path.path());

        let mut url = Url::parse(&url_string).map_err(|_| Error::InvalidUrl(url_string))?;

        {
            let mut pairs = url.query_pairs_mut();
            let serializer = serde_urlencoded::Serializer::new(&mut pairs);

            params
                .serialize(serializer)
                .map_err(|_| Error::InvalidParams)?;
        }

        Ok(url)
    }
}

#[async_trait(?Send)]
pub trait Instance: Sized {
    fn cli(&self) -> &Cli;
    fn client(&self) -> &LemmyClient;
    fn instance_info(&self) -> &LemmyInstanceInfo;
    fn map_instance_id(&self, instance_id: InstanceId) -> Result<InstanceId>;

    async fn get_posts_and_comment_counts(
        &self,
        community_id: LemmyCommunityId,
        page: u64,
    ) -> Result<Option<Vec<(LemmyPost, u64)>>> {
        assert!(community_id.instance_id == self.instance_info().id);

        let params = GetPosts {
            type_: Some(ListingType::All),
            sort: Some(SortType::New),
            page: Some(page as i64),
            limit: Some(50),
            community_id: Some(community_id.foreign_id),
            community_name: None,
            saved_only: None,
            auth: None,
        };

        let posts = self
            .client()
            .get_with_params(ListPostsPath, params)
            .await?
            .posts;

        let start_date = self.cli().start_date();

        if posts.iter().all(|view| view.post.published < start_date) {
            Ok(None)
        } else {
            Ok(Some(
                posts
                    .into_iter()
                    .filter(|view| {
                        view.post.published > start_date
                        && !view.post.nsfw                 // No NSFW posts
                        && !view.counts.featured_community // No featured posts
                        && !view.post.featured_local
                    })
                    .map(|view| {
                        let comment_count = view.counts.comments as u64;
                        (LemmyPost::from(view, community_id, self), comment_count)
                    })
                    .collect(),
            ))
        }
    }

    async fn get_comments(
        &self,
        post_id: LemmyPostId,
        comment_count: u64,
    ) -> Result<Vec<LemmyComment>> {
        assert!(post_id.community_id.instance_id == self.instance_info().id);

        let common_params = GetComments {
            type_: Some(ListingType::All),
            sort: Some(CommentSortType::New),
            max_depth: None,
            page: None,
            limit: None,
            community_id: Some(post_id.community_id.id),
            community_name: None,
            post_id: Some(post_id.id),
            parent_id: None,
            saved_only: None,
            auth: None,
        };

        let res: Result<_> = try {
            match comment_count {
                0 => Vec::new(),
                1..=300 => {
                    // we just need one request
                    let params = GetComments {
                        max_depth: Some(comment_count as i32 + 1),
                        ..common_params
                    };

                    self.client()
                        .get_with_params(ListCommentsPath, params)
                        .await?
                        .comments
                }
                _ => {
                    let pages = if comment_count % 50 == 0 {
                        comment_count / 50
                    } else {
                        comment_count / 50 + 1
                    };

                    stream::iter(1..=pages)
                        .map(|page| {
                            let params = GetComments {
                                page: Some(page as i64),
                                limit: Some(50),
                                ..common_params.clone()
                            };

                            self.client().get_with_params(ListCommentsPath, params)
                        })
                        .buffer_max_concurrent(50)
                        .map_ok(|res| res.comments)
                        .try_take_while(|c| future::ready(Ok(!c.is_empty())))
                        .flatten_ok()
                        .try_collect::<Vec<_>>()
                        .await?
                }
            }
        };

        match res {
            Ok(comments) => Ok(comments
                .into_iter()
                .map(|view| LemmyComment::try_from(view, post_id, self))
                .collect::<Result<_>>()?),
            Err(Error::Lemmy(e)) if e.error == "temporarily_disabled" => {
                log!(
                    "Comments for post {:?} temporarily disabled: {:?}",
                    post_id,
                    e
                );
                Ok(Vec::new())
            }
            Err(e) => Err(e),
        }
    }

    async fn get_user(&self, user_id: LemmyUserId) -> Result<LemmyUser> {
        assert!(user_id.instance_id == self.instance_info().id);

        let params = GetPersonDetails {
            username: Some(user_id.name),
            limit: Some(1),
            ..Default::default()
        };

        let person_view = self
            .client()
            .get_with_params(GetUserPath, params)
            .await?
            .person_view;

        Ok(LemmyUser::from(person_view, user_id.instance_id))
    }
}

pub struct MainInstance {
    cli: Arc<Cli>,
    pub client: LemmyClient,
    pub instance_info: LemmyInstanceInfo,
    pub instance_ids: HashMap<Arc<str>, InstanceId>,
}

impl MainInstance {
    pub async fn new(cli: Arc<Cli>, domain: Arc<str>, login: Login) -> Result<MainInstance> {
        let client = LemmyClient::build_reqwest_client(&domain)?;

        let jwt = client
            .post(format!("https://{}/api/v3/user/login", domain))
            .json(&login)
            .send()
            .await?
            .json::<LoginResponse>()
            .await?
            .jwt
            .ok_or(Error::LoginFailed)?;

        let params = GetSite {
            auth: Some(jwt.clone()),
        };

        let site: GetSiteResponse = client
            .get(format!("https://{}/api/v3/site", domain))
            .query(&params)
            .send()
            .await?
            .json()
            .await?;

        let instance_id = site.site_view.site.instance_id;

        let client = LemmyClient::new(
            cli.clone(),
            domain.clone(),
            &site.site_view,
            client,
            Some(jwt),
        )
        .await?;

        let instance_info = LemmyInstanceInfo::from(site.site_view, domain.clone(), None);

        let instance_ids = client
            .get(GetInstancesPath)
            .await?
            .federated_instances
            .ok_or(Error::GetInstancesFailed)?
            .linked
            .into_iter()
            .filter(|i| i.software.as_deref() == Some("lemmy"))
            .map(|i| (i.domain.into(), i.id))
            .chain(Some((domain.clone(), instance_id)))
            .collect::<HashMap<_, _>>();

        Ok(MainInstance {
            cli,
            client,
            instance_info,
            instance_ids,
        })
    }

    pub async fn get_communities(
        &self,
        page: u64,
        instances: &HashMap<InstanceId, LazyForeignInstance<'_>>,
    ) -> Result<Vec<(LemmyCommunityId, CommunityView)>> {
        let params = ListCommunities {
            type_: Some(ListingType::All),
            sort: Some(SortType::TopAll),
            show_nsfw: Some(false),
            page: Some(page as i64),
            limit: Some(50),
            auth: None,
        };

        self.client
            .get_with_params(ListCommunitiesPath, params)
            .await?
            .communities
            .into_stream()
            .map(|view| async move {
                LemmyCommunityId::try_from(&view.community, self, instances)
                    .await
                    .map_inner(|id| (id, view))
            })
            .buffer_unordered(200)
            .try_filter_map(|x| future::ready(Ok(x)))
            .try_collect()
            .await
    }
}

impl Instance for MainInstance {
    fn cli(&self) -> &Cli {
        &self.cli
    }

    fn client(&self) -> &LemmyClient {
        &self.client
    }

    fn instance_info(&self) -> &LemmyInstanceInfo {
        &self.instance_info
    }

    fn map_instance_id(&self, instance_id: InstanceId) -> Result<InstanceId> {
        Ok(instance_id)
    }
}

pub struct ForeignInstance {
    cli: Arc<Cli>,
    pub client: LemmyClient,
    pub instance_info: LemmyInstanceInfo,
    pub instance_id_map: HashMap<InstanceId, InstanceId>,
}

impl ForeignInstance {
    pub async fn new(
        cli: Arc<Cli>,
        domain: Arc<str>,
        main_instance: &MainInstance,
    ) -> Result<ForeignInstance> {
        let client = LemmyClient::build_reqwest_client(&domain)?;

        let params = GetSite::default();

        let site: GetSiteResponse = client
            .get(format!("https://{}/api/v3/site", domain))
            .query(&params)
            .send()
            .await?
            .json()
            .await?;

        let client =
            LemmyClient::new(cli.clone(), domain.clone(), &site.site_view, client, None).await?;

        let instance_id = *main_instance
            .instance_ids
            .get(&domain)
            .ok_or(Error::InstanceNotFound)?;

        let foreign_instance_id = site.site_view.site.instance_id;

        let instance_info =
            LemmyInstanceInfo::from(site.site_view, domain.clone(), Some(instance_id));

        let instance_id_map = client
            .get(GetInstancesPath)
            .await?
            .federated_instances
            .ok_or(Error::GetInstancesFailed)?
            .linked
            .into_iter()
            .filter(|i| i.software.as_deref() == Some("lemmy"))
            .filter_map(|i| {
                let main_id = if i.domain.as_str() == main_instance.instance_info.domain.as_ref() {
                    Some(main_instance.instance_info.id)
                } else {
                    main_instance.instance_ids.get(i.domain.as_str()).copied()
                };
                main_id.map(|id| (i.id, id))
            })
            .chain(Some((foreign_instance_id, instance_id)))
            .collect::<HashMap<_, _>>();

        Ok(ForeignInstance {
            cli,
            client,
            instance_info,
            instance_id_map,
        })
    }
}

impl Instance for ForeignInstance {
    fn cli(&self) -> &Cli {
        &self.cli
    }

    fn client(&self) -> &LemmyClient {
        &self.client
    }

    fn instance_info(&self) -> &LemmyInstanceInfo {
        &self.instance_info
    }

    fn map_instance_id(&self, instance_id: InstanceId) -> Result<InstanceId> {
        self.instance_id_map
            .get(&instance_id)
            .copied()
            .ok_or(Error::InstanceNotFound)
    }
}

pub struct LazyForeignInstance<'a>(
    Lazy<Result<ForeignInstance>, LocalBoxFuture<'a, Result<ForeignInstance>>>,
);

impl<'a> LazyForeignInstance<'a> {
    pub fn new(cli: Arc<Cli>, domain: Arc<str>, main_instance: &'a MainInstance) -> Self {
        LazyForeignInstance(Lazy::new(
            ForeignInstance::new(cli, domain.clone(), main_instance).boxed_local(),
        ))
    }

    pub async fn get(&self) -> Result<&ForeignInstance> {
        (&self.0).await.as_ref().map_err(Clone::clone)
    }

    pub fn into_inner(self) -> Result<ForeignInstance> {
        self.0.into_inner().ok_or(Error::ClientNotReady).flatten()
    }
}
