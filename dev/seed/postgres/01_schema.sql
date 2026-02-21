CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer TEXT NOT NULL,
    amount NUMERIC(10, 2),
    notes TEXT,
    shipped BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE order_items (
    order_id INT NOT NULL,
    item_id INT NOT NULL,
    product TEXT NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (order_id, item_id)
);
