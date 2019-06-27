table! {
    subscription_event (event_id) {
        event_id -> Integer,
        subscription_id -> Text,
        task_id -> Text,
        subtask_id -> Nullable<Text>,
        ts -> Timestamp,
        event_type -> Text,
        event_desc -> Nullable<Text>,
    }
}

table! {
    subscription_subtask (subscription_id, task_id, subtask_id) {
        subscription_id -> Text,
        task_id -> Text,
        subtask_id -> Text,
        price_gnt -> Nullable<Double>,
        deadline -> Nullable<Timestamp>,
    }
}

table! {
    subscription_tasks (subscription_id, task_id) {
        subscription_id -> Text,
        task_id -> Text,
        deadline -> Nullable<Timestamp>,
        resource_size -> Nullable<Integer>,
        estimated_memory -> Nullable<BigInt>,
        max_price_gnt -> Nullable<Double>,
    }
}

table! {
    subscriptions (subscription_id) {
        hub_id -> Text,
        session_id -> Integer,
        subscription_id -> Text,
    }
}

joinable!(subscription_tasks -> subscriptions (subscription_id));

allow_tables_to_appear_in_same_query!(
    subscription_event,
    subscription_subtask,
    subscription_tasks,
    subscriptions,
);
