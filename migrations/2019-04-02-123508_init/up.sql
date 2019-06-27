-- Your SQL goes here
CREATE TABLE subscriptions(
    hub_id VARCHAR (50) NOT NULL,
    session_id INTEGER NOT NULL,
    subscription_id VARCHAR(50) NOT NULL,
    CONSTRAINT subscriptions_pk PRIMARY KEY (subscription_id)
);

CREATE TABLE subscription_tasks(
    subscription_id VARCHAR(50) NOT NULL,
    task_id VARCHAR(200) NOT NULL,
    deadline DATETIME,
    resource_size INTEGER,
    estimated_memory BIGINT,
    max_price_gnt NUMBER,
    CONSTRAINT subscription_task_pk PRIMARY KEY (subscription_id, task_id),
    CONSTRAINT subscription_task_fk1 FOREIGN KEY (subscription_id) REFERENCES  subscriptions(subscription_id)
);


CREATE TABLE subscription_subtask(
    subscription_id VARCHAR(50) NOT NULL ,
    task_id VARCHAR(200) NOT NULL,
    subtask_id VARCHAR2(200) NOT NULL,
    price_gnt NUMBER,
    deadline DATETIME,
    CONSTRAINT subscription_subtask_pk PRIMARY KEY (subscription_id, task_id, subtask_id),
    CONSTRAINT subscription_subtask_fk1 FOREIGN KEY (subscription_id, task_id) references subscription_tasks(subscription_id, task_id)
);

CREATE TABLE subscription_event(
    event_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    subscription_id VARCHAR(50) NOT NULL ,
    task_id VARCHAR(200) NOT NULL,
    subtask_id VARCHAR(200),
    ts timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_type VARCHAR(50) not null,
    event_desc VARCHAR(500),
    CONSTRAINT subscription_subtask_fk1 FOREIGN KEY (subscription_id, task_id) references subscription_tasks(subscription_id, task_id)
);

