#![feature(async_fn_in_trait)]
#![feature(binary_heap_into_iter_sorted)]
#![feature(const_option)]
#![feature(iter_next_chunk)]
#![feature(lazy_cell)]
#![feature(result_flattening)]
#![feature(try_blocks)]
#![feature(yeet_expr)]

mod cli;
mod error;
mod ext;
mod instance;
mod lemmy;
mod log;
mod path;

use std::{
    collections::{BinaryHeap, HashMap},
    iter,
    sync::Arc,
};

use clap::Parser as _;
use dotenv_codegen::dotenv;
use futures::{
    future::{self, join_all},
    stream,
    StreamExt, FutureExt as _,
};
use indexmap::IndexMap;
use itertools::Itertools;
use lemmy_api_common::person::Login;
use scc::HashSet;

use crate::{
    cli::Cli,
    ext::{IntoStream as _, StreamExt as _},
    instance::{Instance, LazyForeignInstance, MainInstance},
    lemmy::{CommunityByActivity, LemmyCommunity, LemmyCommunityId, MaybeLemmyUser},
    log::Logger,
};

#[tokio::main]
async fn main() {
    let cli = Arc::new(Cli::parse());

    let _guard = Logger::initialize(cli.logfile());

    // Set up main client
    let login = Login {
        username_or_email: dotenv!("LEMMY_USERNAME").into(),
        password: dotenv!("LEMMY_PASSWORD").into(),
        totp_2fa_token: None,
    };

    let main_instance =
        MainInstance::new(cli.clone(), dotenv!("LEMMY_DOMAIN").into(), login).await.unwrap();

    log!("Fetching instances");

    let instances = main_instance
        .instance_ids
        .iter()
        .map(|(domain, id)| {
            (
                *id,
                LazyForeignInstance::new(cli.clone(), domain.clone(), &main_instance),
            )
        })
        .collect::<HashMap<_, _>>();

    log!("Instances: {}", instances.len() + 1);

    let communities_bar = Logger::create_progress_spinner("Fetching communities");

    let community_views = (1..)
        .into_stream()
        .map(|page| main_instance.get_community_views(page))
        .buffer_max_concurrent(20)
        .take_while(|c| future::ready(!c.is_empty()))
        .inspect(|_| communities_bar.inc())
        .flat_map(stream::iter)
        .filter(|view| future::ready(view.counts.users_active_half_year > 0))
        .map(|view| (view.community.id, view))
        .collect::<Vec<_>>()
        .await;

    let community_view_count = community_views.len();

    let community_views_per_instance = community_views.into_iter().into_group_map();

    communities_bar.finish();

    log!("Communities: {}", community_view_count);

    let communities_bar =
        Logger::create_progress_bar(community_view_count as u64, "Filtering communities");

    let communities = community_views_per_instance
        .into_values()
        .sorted_by_key(|views| views.len())
        .rev()
        .into_stream()
        .map(|views| {
            views
                .into_stream()
                .map(|view| {
                    let main_instance = &main_instance;
                    let instances = &instances;
                    async move {
                        LemmyCommunityId::try_from(&view.community, main_instance, instances)
                            .await
                            .map(|id| (id, view))
                    }
                })
                .buffer_unordered(20)
                .flat_map(stream::iter)
                .map(CommunityByActivity::from)
        })
        .flatten_unordered(200)
        .inspect(|_| communities_bar.inc())
        .collect::<BinaryHeap<_>>()
        .await
        .into_iter_sorted()
        .take(10_000)
        .map(|c| {
            let post_count = c.view.counts.posts as u64;
            (c.id, (LemmyCommunity::from(c), post_count))
        })
        .collect::<IndexMap<_, _>>();

    communities_bar.finish();

    log!("Filtered communities: {}", communities.len());

    cli.write_csv(communities.values().map(|(c, _)| c), "communities.csv")
        .await.unwrap();

    let user_ids = HashSet::new();

    let overestimated_post_count = communities.values().map(|(_, c)| c).sum();

    let posts_bar = Logger::create_progress_bar(overestimated_post_count, "Fetching posts");

    let posts = communities
        .iter()
        .rev()
        .into_stream()
        .map(|(id, (community, post_count))| {
            let pages = if post_count % 50 == 0 {
                post_count / 50
            } else {
                post_count / 50 + 1
            };

            let main_instance = &main_instance;
            let instances = &instances;
            let user_ids = &user_ids;
            let posts_bar = &posts_bar;

            async move {
                let posts = stream::iter(1..=pages)
                    .map(|page| async move {
                        if id.instance_id == main_instance.instance_info.id {
                            main_instance
                                .get_posts_and_comment_counts(community.id, page)
                                .await
                        } else {
                            let instance = match instances
                                .get(&id.instance_id)
                                .expect("Instances should be present")
                                .get()
                                .await {
                                Ok(instance) => instance,
                                Err(_) => {
                                    log!(trace, "Instance not found: {:?}", id.instance_id);
                                    return Some(Vec::new())
                                },
                            };

                            instance
                                .get_posts_and_comment_counts(community.id, page)
                                .await
                        }
                    })
                    .buffer_max_concurrent(20)
                    .take_while(|posts| future::ready(posts.is_some()))
                    .filter_map(future::ready)
                    .flat_map(stream::iter)
                    .then(|(p, c)| async move {
                        posts_bar.inc();

                        if let Some(id) = p.creator_id().cloned() {
                            _ = user_ids.insert_async(id).await;
                        }

                        (p, c)
                    })
                    .collect::<Vec<_>>()
                    .await;

                let correction = posts.len() as i64 - *post_count as i64;
                posts_bar.add_len(correction);

                posts
            }
        })
        .buffer_unordered(200)
        .flat_map(stream::iter)
        .map(|(p, c)| (p.id, (p, c)))
        .collect::<HashMap<_, _>>()
        .await;

    posts_bar.finish();

    log!("Posts: {}", posts.len());

    for (i, chunk) in posts
        .values()
        .map(|(p, _)| p)
        .chunks(100_000)
        .into_iter()
        .enumerate()
    {
        cli.write_csv(chunk, format!("posts_{}.csv", i).as_str())
            .await.unwrap();
    }

    log!("Users: {}", user_ids.len());

    let posts_per_instance = posts
        .iter()
        .map(|(id, (_, c))| (id.community_id.instance_id, (id, *c)))
        .into_group_map();

    let comments_bar = Logger::create_progress_bar(posts.len() as u64, "Fetching comments");

    let comment_count = {
        let mut comment_count = 0;
        let comment_chunks =
            join_all(posts_per_instance.into_iter().map(|(instance_id, posts)| {
                let main_instance = &main_instance;
                let instances = &instances;
                let user_ids = &user_ids;
                let comments_bar = &comments_bar;

                async move {
                    stream::iter(posts)
                        .map(|(&id, comment_count)| async move {
                            if instance_id == main_instance.instance_info.id {
                                main_instance.get_comments(id, comment_count).await
                            } else {
                                let instance = match instances
                                    .get(&instance_id)
                                    .expect("Instances should be present")
                                    .get()
                                    .await {
                                    Ok(instance) => instance,
                                    Err(_) => {
                                        log!(trace, "Instance not found: {:?}", instance_id);
                                        return Vec::new()
                                    }
                                };

                                instance
                                    .get_comments(id, comment_count)
                                    .await
                            }
                        })
                        .buffer_unordered(50)
                        .inspect(|_| comments_bar.inc())
                        .flat_map(stream::iter)
                        .then(|c| async move {
                            if let Some(id) = c.creator_id().cloned() {
                                _ = user_ids.insert_async(id).await;
                            }

                            c
                        })
                        .collect::<Vec<_>>()
                        .await
                }
            }))
            .await
            .into_iter()
            .inspect(|comments| comment_count += comments.len())
            .flatten()
            .chunks(100_000);

        for (i, chunk) in comment_chunks.into_iter().enumerate() {
            cli.write_csv(chunk, format!("comments_{}.csv", i).as_str())
                .await.unwrap();
        }

        comment_count
    };

    comments_bar.finish();

    log!("Comments: {}", comment_count);

    let users_bar = Logger::create_progress_bar(user_ids.len() as u64, "Fetching users");

    let future_users = {
        let mut future_users = HashMap::new();

        user_ids.scan(|id| {
            let main_instance = &main_instance;
            let instances = &instances;
            future_users.entry(id.clone()).or_insert_with(|| {
                let id = id.clone();
                async move {
                    if id.instance_id == main_instance.instance_info.id {
                        main_instance.get_user(id).await.ok() // Drop users where request fails
                    } else if let Some(instance) = instances.get(&id.instance_id) {
                        if let Ok(instance) = instance.get().await {
                            instance.get_user(id).await.ok() // Drop users where request fails
                        } else {
                            None // Instance is not reachable
                        }
                    } else {
                        None
                    }
                }
            });
        });

        future_users
    };

    let users = future_users
        .into_stream()
        .map(|(id, fut)| {
            fut.map(|u| {
                let user = MaybeLemmyUser::from((&id, u));
                (id, user)
            })
        })
        .buffer_unordered(200)
        .inspect(|_| users_bar.inc())
        .collect::<HashMap<_, _>>()
        .await;

    users_bar.finish();

    log!("Users: {}", users.len());

    for (i, chunk) in users.values().chunks(100_000).into_iter().enumerate() {
        cli.write_csv(chunk, format!("users_{}.csv", i).as_str())
            .await.unwrap();
    }

    let instances = instances
        .into_iter()
        .filter_map(|(id, lazy_instance)| lazy_instance.into_inner().ok().map(|i| (id, i)))
        .collect::<HashMap<_, _>>();

    log!("Instances: {}", instances.len());

    let instance_infos = iter::once(&main_instance.instance_info)
        .chain(instances.values().map(|i| &i.instance_info));

    cli.write_csv(instance_infos, "instances.csv").await.unwrap();
}
