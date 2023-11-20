use std::{
    cmp::Ordering,
    collections::{binary_heap::PeekMut, BinaryHeap},
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{
    stream::{self, Fuse, FusedStream, FuturesUnordered, Iter},
    Stream, StreamExt as _, TryStream,
};
use futures_core::ready;
use pin_project_lite::pin_project;

pub trait HasInner {
    type Inner;
    type Wrapped<T>: HasInner<Inner = T>;

    fn map_inner<U, F>(self, f: F) -> Self::Wrapped<U>
    where
        F: FnOnce(Self::Inner) -> U;
}

impl<T> HasInner for Option<Option<T>> {
    type Inner = T;
    type Wrapped<U> = Option<Option<U>>;

    fn map_inner<U, F>(self, f: F) -> Self::Wrapped<U>
    where
        F: FnOnce(Self::Inner) -> U,
    {
        self.map(|inner| inner.map(f))
    }
}

impl<T, E> HasInner for Option<Result<T, E>> {
    type Inner = T;
    type Wrapped<U> = Option<Result<U, E>>;

    fn map_inner<U, F>(self, f: F) -> Self::Wrapped<U>
    where
        F: FnOnce(Self::Inner) -> U,
    {
        self.map(|inner| inner.map(f))
    }
}

impl<T, E> HasInner for Result<Option<T>, E> {
    type Inner = T;
    type Wrapped<U> = Result<Option<U>, E>;

    fn map_inner<U, F>(self, f: F) -> Self::Wrapped<U>
    where
        F: FnOnce(Self::Inner) -> U,
    {
        self.map(|inner| inner.map(f))
    }
}

impl<T, E1, E2> HasInner for Result<Result<T, E2>, E1> {
    type Inner = T;
    type Wrapped<U> = Result<Result<U, E2>, E1>;

    fn map_inner<U, F>(self, f: F) -> Self::Wrapped<U>
    where
        F: FnOnce(Self::Inner) -> U,
    {
        self.map(|inner| inner.map(f))
    }
}

pub trait IntoStream: IntoIterator {
    fn into_stream(self) -> Iter<Self::IntoIter>
    where
        Self: Sized,
    {
        stream::iter(self)
    }
}

impl<T: ?Sized + IntoIterator> IntoStream for T {}

pub trait StreamExt: Stream {
    fn buffer_max_concurrent(self, n: usize) -> BufferMaxConcurrent<Self>
    where
        Self::Item: Future,
        Self: Sized,
    {
        BufferMaxConcurrent::new(self, n)
    }
}

impl<T: ?Sized + Stream> StreamExt for T {}

pub trait TryStreamExt: TryStream {
    fn flatten_ok(self) -> FlattenOk<Self>
    where
        Self::Ok: IntoIterator,
        Self: Sized,
    {
        FlattenOk::new(self)
    }
}

impl<T: ?Sized + TryStream> TryStreamExt for T {}

pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    #[derive(Debug)]
    struct OrderWrapper<T> {
        #[pin]
        data: T, // A future or a future's output
        index: isize,
    }
}

impl<T> PartialEq for OrderWrapper<T> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl<T> Eq for OrderWrapper<T> {}

impl<T> PartialOrd for OrderWrapper<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for OrderWrapper<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max heap, so compare backwards here.
        other.index.cmp(&self.index)
    }
}

impl<T> Future for OrderWrapper<T>
where
    T: Future,
{
    type Output = OrderWrapper<T::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let index = self.index;
        self.project().data.poll(cx).map(|output| OrderWrapper {
            data: output,
            index,
        })
    }
}

#[must_use = "streams do nothing unless polled"]
struct FuturesOrdered<T: Future> {
    in_progress_queue: FuturesUnordered<OrderWrapper<T>>,
    queued_outputs: BinaryHeap<OrderWrapper<T::Output>>,
    next_incoming_index: isize,
    next_outgoing_index: isize,
}

impl<T: Future> Unpin for FuturesOrdered<T> {}

impl<Fut: Future> FuturesOrdered<Fut> {
    fn new() -> Self {
        Self {
            in_progress_queue: FuturesUnordered::new(),
            queued_outputs: BinaryHeap::new(),
            next_incoming_index: 0,
            next_outgoing_index: 0,
        }
    }

    fn in_progress_len(&self) -> usize {
        self.in_progress_queue.len()
    }

    fn len(&self) -> usize {
        self.in_progress_queue.len() + self.queued_outputs.len()
    }

    fn push_back(&mut self, future: Fut) {
        let wrapped = OrderWrapper {
            data: future,
            index: self.next_incoming_index,
        };
        self.next_incoming_index += 1;
        self.in_progress_queue.push(wrapped);
    }
}

impl<Fut: Future> Stream for FuturesOrdered<Fut> {
    type Item = Fut::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;

        // Check to see if we've already received the next value
        if let Some(next_output) = this.queued_outputs.peek_mut() {
            if next_output.index == this.next_outgoing_index {
                this.next_outgoing_index += 1;
                return Poll::Ready(Some(PeekMut::pop(next_output).data));
            }
        }

        loop {
            match ready!(this.in_progress_queue.poll_next_unpin(cx)) {
                Some(output) => {
                    if output.index == this.next_outgoing_index {
                        this.next_outgoing_index += 1;
                        return Poll::Ready(Some(output.data));
                    } else {
                        this.queued_outputs.push(output)
                    }
                }
                None => return Poll::Ready(None),
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<Fut: Future> FusedStream for FuturesOrdered<Fut> {
    fn is_terminated(&self) -> bool {
        self.in_progress_queue.is_terminated() && self.queued_outputs.is_empty()
    }
}

pin_project! {
    #[must_use = "streams do nothing unless polled"]
    pub struct BufferMaxConcurrent<S>
    where
        S: Stream,
        S::Item: Future,
    {
        #[pin]
        stream: Fuse<S>,
        in_progress_queue: FuturesOrdered<S::Item>,
        max: usize,
    }
}

impl<S> BufferMaxConcurrent<S>
where
    S: Stream,
    S::Item: Future,
{
    pub(super) fn new(stream: S, n: usize) -> Self {
        Self {
            stream: stream.fuse(),
            in_progress_queue: FuturesOrdered::new(),
            max: n,
        }
    }
}

impl<S> Stream for BufferMaxConcurrent<S>
where
    S: Stream,
    S::Item: Future,
{
    type Item = <S::Item as Future>::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // First up, try to spawn off as many futures as possible by filling up
        // our queue of futures.
        while this.in_progress_queue.in_progress_len() < *this.max {
            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(fut)) => this.in_progress_queue.push_back(fut),
                Poll::Ready(None) | Poll::Pending => break,
            }
        }

        // Attempt to pull the next value from the in_progress_queue
        let res = this.in_progress_queue.poll_next_unpin(cx);
        if let Some(val) = ready!(res) {
            return Poll::Ready(Some(val));
        }

        // If more values are still coming from the stream, we're not done yet
        if this.stream.is_done() {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let queue_len = self.in_progress_queue.len();
        let (lower, upper) = self.stream.size_hint();
        let lower = lower.saturating_add(queue_len);
        let upper = match upper {
            Some(x) => x.checked_add(queue_len),
            None => None,
        };
        (lower, upper)
    }
}

impl<St> FusedStream for BufferMaxConcurrent<St>
where
    St: Stream,
    St::Item: Future,
{
    fn is_terminated(&self) -> bool {
        self.stream.is_done() && self.in_progress_queue.is_terminated()
    }
}

pin_project! {
    #[must_use = "streams do nothing unless polled"]
    pub struct FlattenOk<S>
    where
        S: TryStream,
        S::Ok: IntoIterator,
    {
        #[pin]
        stream: S,
        next: Option<<S::Ok as IntoIterator>::IntoIter>,
    }
}

impl<S> FlattenOk<S>
where
    S: TryStream,
    S::Ok: IntoIterator,
{
    pub(super) fn new(stream: S) -> Self {
        Self { stream, next: None }
    }
}

impl<S> FusedStream for FlattenOk<S>
where
    S: TryStream + FusedStream,
    S::Ok: IntoIterator,
{
    fn is_terminated(&self) -> bool {
        self.next.is_none() && self.stream.is_terminated()
    }
}

impl<S> Stream for FlattenOk<S>
where
    S: TryStream,
    S::Ok: IntoIterator,
{
    type Item = Result<<S::Ok as IntoIterator>::Item, S::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        Poll::Ready(loop {
            if let Some(s) = this.next.as_mut() {
                if let Some(item) = s.next() {
                    break Some(Ok(item));
                } else {
                    *this.next = None;
                }
            } else if let Some(s) = ready!(this.stream.as_mut().try_poll_next(cx)?) {
                *this.next = Some(s.into_iter());
            } else {
                break None;
            }
        })
    }
}
