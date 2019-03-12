use actix::prelude::*;

enum Either<F: ActorFuture> {
    Pending(F),
    Ready(F::Item),
    None,
}

impl<F: ActorFuture> Either<F> {
    fn try_poll(
        &mut self,
        srv: &mut F::Actor,
        ctx: &mut <<F as ActorFuture>::Actor as Actor>::Context,
    ) -> Result<futures::Async<()>, F::Error> {
        let r = match self {
            Either::Pending(f) => f.poll(srv, ctx),
            Either::Ready(it) => return Ok(futures::Async::Ready(())),
            Either::None => panic!("unexpected state none"),
        };
        let it = match r {
            Err(e) => return Err(e),
            Ok(futures::Async::NotReady) => return Ok(futures::Async::NotReady),
            Ok(futures::Async::Ready(it)) => it,
        };
        ::std::mem::replace(self, Either::Ready(it));
        Ok(futures::Async::Ready(()))
    }

    fn unwrap(&mut self) -> F::Item {
        match ::std::mem::replace(self, Either::None) {
            Either::Ready(it) => it,
            _ => panic!("invalid unwrap"),
        }
    }
}

struct Join<A: Actor, E, L: ActorFuture<Actor = A, Error = E>, R: ActorFuture<Actor = A, Error = E>>
{
    left: Either<L>,
    right: Either<R>,
}

pub fn join_act_fut<
    A: Actor,
    E,
    L: ActorFuture<Actor = A, Error = E>,
    R: ActorFuture<Actor = A, Error = E>,
>(
    left: L,
    right: R,
) -> impl ActorFuture<Actor = A, Item = (L::Item, R::Item), Error = E> {
    let left = Either::Pending(left);
    let right = Either::Pending(right);

    Join { left, right }
}

impl<A: Actor, E, L: ActorFuture<Actor = A, Error = E>, R: ActorFuture<Actor = A, Error = E>>
    ActorFuture for Join<A, E, L, R>
{
    type Item = (L::Item, R::Item);
    type Error = E;
    type Actor = A;

    fn poll(
        &mut self,
        srv: &mut Self::Actor,
        ctx: &mut <Self::Actor as Actor>::Context,
    ) -> Result<futures::Async<Self::Item>, Self::Error> {
        match (
            self.left.try_poll(srv, ctx)?,
            self.right.try_poll(srv, ctx)?,
        ) {
            (futures::Async::Ready(_), futures::Async::Ready(_)) => (),
            _ => return Ok(futures::Async::NotReady),
        }

        Ok(futures::Async::Ready((
            self.left.unwrap(),
            self.right.unwrap(),
        )))
    }
}
