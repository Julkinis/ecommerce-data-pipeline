INSERT INTO products (product_name, category, price)
VALUES
    ('Wireless Mouse', 'Electronics', 25.99),
    ('Mechanical Keyboard', 'Electronics', 79.99),
    ('USB-C Hub', 'Electronics', 45.50),
    ('Coffee Mug', 'Home', 12.00),
    ('Desk Lamp', 'Home', 34.90),
    ('Notebook', 'Stationery', 4.50),
    ('Pen Set', 'Stationery', 8.99)
ON CONFLICT (product_name) DO NOTHING;
