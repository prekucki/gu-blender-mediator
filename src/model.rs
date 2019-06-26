use diesel::prelude::*;
use super::schema::{subscriptions, subscription_tasks, subscription_event};

#[derive(Queryable, Insertable, Debug)]
pub struct Subscription {
    pub hub_id : String,
    pub session_id : i32,
    pub subscription_id : String
}

#[derive(Queryable, Insertable, Debug)]
pub struct SubscriptionTask {
    pub subscription_id : String,
    pub task_id : String,
    pub deadline : Option<chrono::NaiveDateTime>,
    pub resource_size : Option<i32>,
    pub estimated_memory : Option<i64>,
    pub max_price_gnt : Option<f64>
}

#[derive(Queryable, Debug)]
pub struct SubscriptionEvent {
    pub event_id : i32,
    pub subscription_id : String,
    pub task_id : String,
    pub subtask_id : Option<String>,
    pub ts : chrono::NaiveDateTime,
    pub event_type : String,
    pub event_desc : Option<String>
}


#[derive(Insertable)]
#[table_name="subscription_event"]
pub struct NewSubscriptionEvent {
    pub subscription_id : String,
    pub task_id : String,
    pub subtask_id : Option<String>,
    pub event_type : String,
    pub event_desc : Option<String>
}

//pub struct

pub fn establish_connection() -> SqliteConnection {
    use std::env;

    let database_url = env::var("DATABASE_URL").unwrap();

    SqliteConnection::establish(&database_url)
        .expect(&format!("Error connecting to {}", database_url))
}

#[cfg(test)]
#[test]
fn test_insert() {
    let connection = establish_connection();
    use super::model::SubscriptionEvent;
    use super::schema::subscription_event::dsl::*;

    let data = NewSubscriptionEvent {
        subscription_id: super::keygen::gen_subscription_id().to_string(),
        task_id: "smok-123".into(),
        subtask_id: None,
        event_type: "Test".into(),
        event_desc: Some("test txt123".into())
    };

    diesel::insert_into(subscription_event)
        .values(&data)
        .execute(&connection)
        .expect("Error saving");


}

#[cfg(test)]
#[test]
fn test_query() {
    let connection = establish_connection();
    use super::model::SubscriptionEvent;
    use super::schema::subscription_event::dsl::*;

    let events = subscription_event.load::<SubscriptionEvent>(&connection).unwrap();

    for ev in events {
        eprintln!("id={}, {:?}", ev.event_id, ev);
    }

    {
        use super::schema::subscriptions::dsl::*;

        let _ = subscriptions.load::<Subscription>(&connection).unwrap();
    }

}