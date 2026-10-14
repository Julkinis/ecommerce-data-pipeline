CREATE TABLE IF NOT EXISTS products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL UNIQUE,
    category VARCHAR(50) NOT NULL,
    price NUMERIC(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    order_date TIMESTAMP NOT NULL,
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,
    revenue NUMERIC(12, 2) NOT NULL,
    source_order_id TEXT UNIQUE
);
