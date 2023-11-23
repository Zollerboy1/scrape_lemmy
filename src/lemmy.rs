use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use chrono::NaiveDateTime;
use lemmy_api_common::{
    community::GetCommunity,
    lemmy_db_schema::{
        newtypes::{CommentId, CommunityId, InstanceId, PostId},
        source::community::Community,
    },
    lemmy_db_views::structs::{CommentView, PostView, SiteView},
    lemmy_db_views_actor::structs::{CommunityView, PersonView},
};
use serde::Serialize;

use crate::{
    error::Error,
    instance::{GetCommunityPath, Instance, LazyForeignInstance, MainInstance},
    log,
};

serde_with::with_prefix!(prefix_community "community_");
serde_with::with_prefix!(prefix_creator "creator_");
serde_with::with_prefix!(prefix_post "post_");

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize)]
pub struct LemmyInstanceInfo {
    pub id: InstanceId,
    pub domain: Arc<str>,
    pub name: String,
}

impl LemmyInstanceInfo {
    pub fn new(id: InstanceId, domain: Arc<str>, name: String) -> Self {
        LemmyInstanceInfo { id, domain, name }
    }

    pub fn from(view: SiteView, domain: Arc<str>, instance_id: Option<InstanceId>) -> Self {
        LemmyInstanceInfo::new(
            instance_id.unwrap_or(view.site.instance_id),
            domain,
            view.site.name.clone(),
        )
    }
}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Serialize)]
pub struct LemmyCommunityId {
    pub id: CommunityId,
    pub instance_id: InstanceId,
    #[serde(skip)]
    pub foreign_id: CommunityId,
}

impl LemmyCommunityId {
    pub fn new(id: CommunityId, instance_id: InstanceId, foreign_id: CommunityId) -> Self {
        LemmyCommunityId {
            id,
            instance_id,
            foreign_id,
        }
    }

    pub async fn try_from(
        community: &Community,
        main_instance: &MainInstance,
        instances: &HashMap<InstanceId, LazyForeignInstance<'_>>,
    ) -> Option<Self> {
        let (client, domain) = if community.instance_id == main_instance.instance_info.id {
            (
                main_instance.client(),
                main_instance.instance_info.domain.as_ref(),
            )
        } else {
            let Some(instance) = instances.get(&community.instance_id) else {
                return None;
            };

            match instance.get().await {
                Ok(instance) => (&instance.client, instance.instance_info.domain.as_ref()),
                _ => {
                    log!(trace, "Instance not found: {:?}", community.instance_id);
                    return None;
                }
            }
        };

        let params = GetCommunity {
            id: None,
            name: Some(community.name.clone()),
            auth: None,
        };

        let foreign_id = match client
            .get_with_params(GetCommunityPath, params)
            .await
            .map(|res| res.community_view.community.id)
        {
            Ok(id) => id,
            Err(Error::Lemmy(_)) | Err(Error::LemmyBug) => {
                log!(
                    trace,
                    "[{}] Community not found: {}",
                    domain,
                    community.name
                );
                return None;
            }
            Err(e) => {
                log!("[{}] Error getting community: {}", domain, e);
                return None;
            }
        };

        Some(LemmyCommunityId::new(
            community.id,
            community.instance_id,
            foreign_id,
        ))
    }
}

#[derive(Debug)]
pub struct CommunityByActivity {
    pub id: LemmyCommunityId,
    pub view: CommunityView,
}

impl CommunityByActivity {
    pub fn activity(&self) -> i64 {
        self.view.counts.users_active_half_year
    }
}

impl PartialEq for CommunityByActivity {
    fn eq(&self, other: &Self) -> bool {
        self.view.community == other.view.community && self.view.counts == other.view.counts
    }
}

impl Eq for CommunityByActivity {}

impl PartialOrd for CommunityByActivity {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CommunityByActivity {
    fn cmp(&self, other: &Self) -> Ordering {
        self.activity().cmp(&other.activity())
    }
}

impl From<(LemmyCommunityId, CommunityView)> for CommunityByActivity {
    fn from(value: (LemmyCommunityId, CommunityView)) -> Self {
        CommunityByActivity {
            id: value.0,
            view: value.1,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize)]
pub struct LemmyCommunity {
    #[serde(flatten)]
    pub id: LemmyCommunityId,
    pub name: String,
    pub created_at: NaiveDateTime,
    pub subscriber_count: u64,
}

impl LemmyCommunity {
    pub fn new(
        id: LemmyCommunityId,
        name: String,
        created_at: NaiveDateTime,
        subscriber_count: u64,
    ) -> Self {
        LemmyCommunity {
            id,
            name,
            created_at,
            subscriber_count,
        }
    }
}

impl From<CommunityByActivity> for LemmyCommunity {
    fn from(value: CommunityByActivity) -> Self {
        LemmyCommunity::new(
            value.id,
            value.view.community.name,
            value.view.community.published,
            value.view.counts.subscribers as u64,
        )
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize)]
pub struct LemmyUserId {
    pub name: String,
    pub instance_id: InstanceId,
}

impl LemmyUserId {
    pub fn new(name: String, instance_id: InstanceId) -> Self {
        LemmyUserId { name, instance_id }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize)]
pub struct LemmyUser {
    #[serde(flatten)]
    pub id: LemmyUserId,
    pub joined_at: NaiveDateTime,
    pub bot_account: bool,
    pub post_score: i64,
    pub comment_score: i64,
}

impl LemmyUser {
    pub fn new(
        id: LemmyUserId,
        joined_at: NaiveDateTime,
        bot_account: bool,
        post_score: i64,
        comment_score: i64,
    ) -> Self {
        LemmyUser {
            id,
            joined_at,
            bot_account,
            post_score,
            comment_score,
        }
    }

    pub fn from(view: PersonView, instance_id: InstanceId) -> Self {
        let person = view.person;

        LemmyUser::new(
            LemmyUserId::new(person.name, instance_id),
            person.published,
            person.bot_account,
            view.counts.post_score,
            view.counts.comment_score,
        )
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize)]
pub struct EmptyLemmyUser {
    #[serde(flatten)]
    pub id: LemmyUserId,
    pub joined_at: (),
    pub bot_account: (),
    pub post_score: (),
    pub comment_score: (),
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum MaybeLemmyUser {
    Lemmy(LemmyUser),
    Empty(EmptyLemmyUser),
}

impl From<LemmyUser> for MaybeLemmyUser {
    fn from(value: LemmyUser) -> Self {
        MaybeLemmyUser::Lemmy(value)
    }
}

impl From<(&LemmyUserId, Option<LemmyUser>)> for MaybeLemmyUser {
    fn from(value: (&LemmyUserId, Option<LemmyUser>)) -> Self {
        match value.1 {
            Some(user) => MaybeLemmyUser::Lemmy(user),
            None => MaybeLemmyUser::Empty(EmptyLemmyUser {
                id: value.0.clone(),
                joined_at: (),
                bot_account: (),
                post_score: (),
                comment_score: (),
            }),
        }
    }
}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Serialize)]
pub struct LemmyPostId {
    pub id: PostId,
    #[serde(flatten, with = "prefix_community")]
    pub community_id: LemmyCommunityId,
}

impl LemmyPostId {
    pub fn new(id: PostId, community_id: LemmyCommunityId) -> Self {
        LemmyPostId { id, community_id }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize)]
#[serde(untagged)]
enum MaybeLemmyUserId {
    Lemmy(LemmyUserId),
    Empty { name: (), instance_id: () },
}

impl From<LemmyUserId> for MaybeLemmyUserId {
    fn from(value: LemmyUserId) -> Self {
        MaybeLemmyUserId::Lemmy(value)
    }
}

impl From<Option<LemmyUserId>> for MaybeLemmyUserId {
    fn from(value: Option<LemmyUserId>) -> Self {
        match value {
            Some(value) => MaybeLemmyUserId::Lemmy(value),
            None => MaybeLemmyUserId::Empty {
                name: (),
                instance_id: (),
            },
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize)]
pub struct LemmyPost {
    #[serde(flatten)]
    pub id: LemmyPostId,
    #[serde(flatten, with = "prefix_creator")]
    creator_id: MaybeLemmyUserId,
    pub created_at: NaiveDateTime,
    pub title: String,
    pub score: i64,
}

impl LemmyPost {
    pub fn new(
        id: LemmyPostId,
        creator_id: Option<LemmyUserId>,
        created_at: NaiveDateTime,
        title: String,
        score: i64,
    ) -> Self {
        LemmyPost {
            id,
            creator_id: creator_id.into(),
            created_at,
            title,
            score,
        }
    }

    pub fn from(view: PostView, community_id: LemmyCommunityId, instance: &impl Instance) -> Self {
        assert!(community_id.instance_id == instance.instance_info().id);

        let creator_id = instance
            .map_instance_id(view.creator.instance_id)
            .ok()
            .map(|id| LemmyUserId::new(view.creator.name, id));

        LemmyPost::new(
            LemmyPostId::new(view.post.id, community_id),
            creator_id,
            view.post.published,
            view.post.name,
            view.counts.score,
        )
    }

    pub fn creator_id(&self) -> Option<&LemmyUserId> {
        match self.creator_id {
            MaybeLemmyUserId::Lemmy(ref id) => Some(id),
            MaybeLemmyUserId::Empty { .. } => None,
        }
    }
}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq, Serialize)]
pub struct LemmyCommentId {
    pub id: CommentId,
    #[serde(flatten, with = "prefix_post")]
    pub post_id: LemmyPostId,
}

impl LemmyCommentId {
    pub fn new(id: CommentId, post_id: LemmyPostId) -> Self {
        LemmyCommentId { id, post_id }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize)]
pub struct LemmyComment {
    #[serde(flatten)]
    pub id: LemmyCommentId,
    #[serde(flatten, with = "prefix_creator")]
    creator_id: MaybeLemmyUserId,
    pub created_at: NaiveDateTime,
    pub score: i64,
    pub child_count: u32,
    parent_id: Option<CommentId>,
}

impl LemmyComment {
    pub fn new(
        id: LemmyCommentId,
        creator_id: Option<LemmyUserId>,
        created_at: NaiveDateTime,
        score: i64,
        child_count: u32,
        parent_id: Option<CommentId>,
    ) -> Self {
        LemmyComment {
            id,
            creator_id: creator_id.into(),
            created_at,
            score,
            child_count,
            parent_id,
        }
    }

    pub fn from(view: CommentView, post_id: LemmyPostId, instance: &impl Instance) -> Self {
        assert!(post_id.community_id.instance_id == instance.instance_info().id);

        let comment_id = view.comment.id;
        let path = view.comment.path;

        let parent_id = try {
            let [comment_id_string, maybe_parent_id_string] =
                path.split('.').rev().next_chunk().ok()?;

            assert_eq!(comment_id_string.parse::<i32>().ok()?, comment_id.0);

            match maybe_parent_id_string {
                "0" => do yeet,
                parent_id_string => parent_id_string.parse().map(CommentId).ok()?,
            }
        };

        let creator_id = instance
            .map_instance_id(view.creator.instance_id)
            .ok()
            .map(|id| LemmyUserId::new(view.creator.name, id));

        LemmyComment::new(
            LemmyCommentId::new(view.comment.id, post_id),
            creator_id,
            view.comment.published,
            view.counts.score,
            view.counts.child_count as u32,
            parent_id,
        )
    }

    pub fn creator_id(&self) -> Option<&LemmyUserId> {
        match self.creator_id {
            MaybeLemmyUserId::Lemmy(ref id) => Some(id),
            MaybeLemmyUserId::Empty { .. } => None,
        }
    }

    #[allow(unused)]
    pub fn parent_id(&self) -> Option<LemmyCommentId> {
        self.parent_id
            .map(|id| LemmyCommentId::new(id, self.id.post_id))
    }
}
