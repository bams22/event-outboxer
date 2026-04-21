CREATE TABLE orders (
    id           UUID         PRIMARY KEY,
    customer_id  VARCHAR(64)  NOT NULL,
    email        VARCHAR(256) NOT NULL,
    total_cents  BIGINT       NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);
