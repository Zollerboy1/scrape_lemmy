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
    future::{self, try_join_all},
    stream::{self, StreamExt as _, TryStreamExt as _},
    TryFutureExt as _,
};
use indexmap::IndexMap;
use indicatif::{ProgressBar, ProgressStyle};
use itertools::Itertools as _;
use lemmy_api_common::person::Login;
use scc::HashSet;

use crate::{
    cli::Cli,
    error::{Error, Result},
    ext::{IntoStream as _, StreamExt as _, TryStreamExt as _},
    instance::{Instance, LazyForeignInstance, MainInstance},
    lemmy::{CommunityByActivity, LemmyCommunity, MaybeLemmyUser},
};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Arc::new(Cli::parse());

    // Set up main client
    let login = Login {
        username_or_email: dotenv!("LEMMY_USERNAME").into(),
        password: dotenv!("LEMMY_PASSWORD").into(),
        totp_2fa_token: None,
    };

    let main_instance =
        MainInstance::new(cli.clone(), dotenv!("LEMMY_DOMAIN").into(), login).await?;

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

    let communities_bar: Arc<_> = ProgressBar::new_spinner()
        .with_style(ProgressStyle::with_template("{msg} {spinner:.gray} {pos:<3.green}").unwrap())
        .with_message("Fetching communities")
        .into();
    log::set_progress_bar(communities_bar.clone());

    let communities = (1..)
        .into_stream()
        .map(|page| main_instance.get_communities(page, &instances))
        .buffer_max_concurrent(100)
        .try_take_while(|c| future::ready(Ok(!c.is_empty())))
        .inspect_ok(|_| communities_bar.inc(1))
        .flatten_ok()
        .try_collect::<Vec<_>>()
        .await?
        .into_stream()
        .map(CommunityByActivity::from)
        .filter(|c| future::ready(c.activity() > 0))
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
    log::clear_progress_bar();

    log!("Communities: {}", communities.len());

    cli.write_csv(communities.values().map(|(c, _)| c), "communities.csv")
        .await?;

    let user_ids = HashSet::new();

    let overestimated_post_count = communities.values().map(|(_, c)| c).sum();

    let posts_bar: Arc<_> = ProgressBar::new(overestimated_post_count)
        .with_style(ProgressStyle::with_template("{msg} {wide_bar} {pos}/{len}").unwrap())
        .with_message("Fetching posts")
        .into();
    log::set_progress_bar(posts_bar.clone());

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
            let posts_bar = posts_bar.clone();

            async move {
                let posts = stream::iter(1..=pages)
                    .map(|page| async move {
                        if id.instance_id == main_instance.instance_info.id {
                            main_instance
                                .get_posts_and_comment_counts(community.id, page)
                                .await
                        } else {
                            instances
                                .get(&id.instance_id)
                                .expect("Instances should be present")
                                .get()
                                .await?
                                .get_posts_and_comment_counts(community.id, page)
                                .await
                        }
                    })
                    .buffer_max_concurrent(50)
                    .try_take_while(|posts| future::ready(Ok(posts.is_some())))
                    .try_filter_map(|x| future::ready(Ok(x)))
                    .flatten_ok()
                    .and_then(|(p, c)| {
                        let posts_bar = posts_bar.clone();
                        async move {
                            posts_bar.inc(1);

                            if let Some(id) = p.creator_id().cloned() {
                                _ = user_ids.insert_async(id).await;
                            }

                            Ok((p, c))
                        }
                    })
                    .try_collect::<Vec<_>>()
                    .await?;

                let correction = post_count - posts.len() as u64;
                posts_bar.update(|state| {
                    let Some(previous_len) = state.len() else {
                        return;
                    };

                    state.set_len(previous_len - correction);
                });

                Ok::<_, Error>(posts)
            }
        })
        .buffer_unordered(200)
        .flatten_ok()
        .map_ok(|(p, c)| (p.id, (p, c)))
        .try_collect::<HashMap<_, _>>()
        .await?;

    posts_bar.finish();
    log::clear_progress_bar();

    log!("Posts: {}", posts.len());

    for (i, chunk) in posts
        .values()
        .map(|(p, _)| p)
        .chunks(100_000)
        .into_iter()
        .enumerate()
    {
        cli.write_csv(chunk, format!("posts_{}.csv", i).as_str())
            .await?;
    }

    log!("Users: {}", user_ids.len());

    let posts_per_instance = posts
        .iter()
        .map(|(id, (_, c))| (id.community_id.instance_id, (id, *c)))
        .into_group_map();

    let comments_bar: Arc<_> = ProgressBar::new(posts.len() as u64)
        .with_style(ProgressStyle::with_template("{msg} {wide_bar} {pos}/{len}").unwrap())
        .with_message("Fetching comments")
        .into();
    log::set_progress_bar(comments_bar.clone());

    let comment_chunks =
        try_join_all(posts_per_instance.into_iter().map(|(instance_id, posts)| {
            let comments_bar = comments_bar.clone();

            let main_instance = &main_instance;
            let instances = &instances;
            let user_ids = &user_ids;

            async move {
                stream::iter(posts)
                    .map(|(&id, comment_count)| async move {
                        if instance_id == main_instance.instance_info.id {
                            main_instance.get_comments(id, comment_count).await
                        } else {
                            instances
                                .get(&instance_id)
                                .expect("Instances should be present")
                                .get()
                                .await?
                                .get_comments(id, comment_count)
                                .await
                        }
                    })
                    .buffer_unordered(50)
                    .inspect_ok(|_| comments_bar.inc(1))
                    .flatten_ok()
                    .and_then(|c| async move {
                        if let Some(id) = c.creator_id().cloned() {
                            _ = user_ids.insert_async(id).await;
                        }

                        Ok(c)
                    })
                    .try_collect::<Vec<_>>()
                    .await
            }
        }))
        .await?
        .into_iter()
        .flatten()
        .chunks(100_000);

    let comment_count = {
        let mut comment_count = 0;
        for (i, chunk) in comment_chunks.into_iter().enumerate() {
            cli.write_csv(chunk, format!("comments_{}.csv", i).as_str())
                .await?;
            comment_count += 1;
        }

        comment_count
    };

    comments_bar.finish();
    log::clear_progress_bar();

    log!("Comments: {}", comment_count);

    let users_bar: Arc<_> = ProgressBar::new(user_ids.len() as u64)
        .with_style(ProgressStyle::with_template("{msg} {wide_bar} {pos}/{len}").unwrap())
        .with_message("Fetching users")
        .into();
    log::set_progress_bar(users_bar.clone());

    let future_users = {
        let mut future_users = HashMap::new();

        user_ids.scan(|id| {
            let main_instance = &main_instance;
            let instances = &instances;
            future_users.entry(id.clone()).or_insert_with(|| {
                let id = id.clone();
                async move {
                    Ok::<_, Error>(if id.instance_id == main_instance.instance_info.id {
                        Some(main_instance.get_user(id).await?)
                    } else if let Some(instance) = instances.get(&id.instance_id) {
                        if let Ok(instance) = instance.get().await {
                            instance.get_user(id).await.ok() // Drop users where request fails
                        } else {
                            None // Instance is not reachable
                        }
                    } else {
                        None
                    })
                }
            });
        });

        future_users
    };

    let users = future_users
        .into_stream()
        .map(|(id, fut)| {
            fut.map_ok(|u| {
                let user = MaybeLemmyUser::from((&id, u));
                (id, user)
            })
        })
        .buffer_unordered(200)
        .inspect_ok(|_| users_bar.inc(1))
        .try_collect::<HashMap<_, _>>()
        .await?;

    users_bar.finish();
    log::clear_progress_bar();

    log!("Users: {}", users.len());

    for (i, chunk) in users.values().chunks(100_000).into_iter().enumerate() {
        cli.write_csv(chunk, format!("users_{}.csv", i).as_str())
            .await?;
    }

    let instances = instances
        .into_iter()
        .filter_map(|(id, lazy_instance)| lazy_instance.into_inner().ok().map(|i| (id, i)))
        .collect::<HashMap<_, _>>();

    log!("Instances: {}", instances.len());

    let instance_infos = iter::once(&main_instance.instance_info)
        .chain(instances.values().map(|i| &i.instance_info));

    cli.write_csv(instance_infos, "instances.csv").await?;

    Ok(())
}