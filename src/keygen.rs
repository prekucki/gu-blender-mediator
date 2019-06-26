/// Generates subscription id for gateway registraton.

use secp256k1::{SecretKey, PublicKey};
use rand::{thread_rng, Rng};
use ethsign;
use gu_client::NodeId;

pub fn gen_subscription_id() -> NodeId {
    let mut rng = thread_rng();
    let secret = SecretKey::random(&mut rng);
    let key = ethsign::SecretKey::from_raw(secret.serialize().as_ref()).unwrap();

    key.public().address().clone().into()
}


#[cfg(test)]
mod test {
    use actix_web::web::get;

    #[test]
    fn test_gen() {
        eprintln!("node_id={:?}", super::gen_subscription_id())
    }

}