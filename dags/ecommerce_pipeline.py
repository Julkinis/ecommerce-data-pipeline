from datetime import datetime
import random
import os

import pendulum
import psycopg2

from airflow.sdk import dag, task, get_current_context


@dag(
    dag_id="ecommerce_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["ecommerce", "portfolio"],
)
def ecommerce_pipeline():

    @task
    def generate_orders():
        """Generate sample orders for the online store."""
        context = get_current_context()
        logical_date = context["logical_date"]
        order_date = logical_date.strftime("%Y-%m-%d")

        orders = []

        for i in range(10):
            product_id = random.randint(1, 7)
            quantity = random.randint(1, 3)

            orders.append({
                "source_order_id": f"{order_date}-{i + 1}",
                "order_date": order_date,
        "product_id": product_id,
                "quantity": quantity,
            })

        return orders

    @task
    def load_orders(orders):
        """Insert orders into PostgreSQL without duplicates."""
        conn = psycopg2.connect(
            host=os.getenv("ECOMMERCE_DB_HOST", "postgres"),
            port=int(os.getenv("ECOMMERCE_DB_PORT", "5432")),
            dbname=os.getenv("ECOMMERCE_DB_NAME", "ecommerce"),
            user=os.getenv("ECOMMERCE_DB_USER", "airflow"),
            password=os.environ["ECOMMERCE_DB_PASSWORD"],
        )

        try:
            with conn:
                with conn.cursor() as cur:
                    for order in orders:
                        cur.execute(
                            """
                            SELECT price
                            FROM products
                            WHERE product_id = %s
                            """,
                            (order["product_id"],),
                        )

                        result = cur.fetchone()
                        if result is None:
                            continue

                        price = result[0]
                        quantity = order["quantity"]
                        revenue = price * quantity

                        cur.execute(
                            """
                            INSERT INTO orders (
                                order_date,
                                product_id,
                                quantity,
                                status,
                                revenue,
                                source_order_id
                            )
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (source_order_id) DO NOTHING
                            """,
                            (
                            datetime.strptime(order["order_date"], "%Y-%m-%d"),
                order["product_id"],
                                quantity,
                                "completed",
                                revenue,
                                order["source_order_id"],
                            ),
                        )
        finally:
            conn.close()

    @task
    def check_data_quality():
        """Validate loaded orders in PostgreSQL."""
        conn = psycopg2.connect(
            host=os.getenv("ECOMMERCE_DB_HOST", "postgres"),
            port=int(os.getenv("ECOMMERCE_DB_PORT", "5432")),
            dbname=os.getenv("ECOMMERCE_DB_NAME", "ecommerce"),
            user=os.getenv("ECOMMERCE_DB_USER", "airflow"),
            password=os.environ["ECOMMERCE_DB_PASSWORD"],
        )

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (
                            WHERE p.product_id IS NULL
                        ) AS missing_products,
                        COUNT(*) FILTER (
                            WHERE ABS(
                                o.revenue - p.price * o.quantity
                            ) > 0.01
                        ) AS revenue_mismatches
                    FROM orders o
                    LEFT JOIN products p
                        ON o.product_id = p.product_id
                """)

                missing_products, revenue_mismatches = cur.fetchone()

                if missing_products > 0 or revenue_mismatches > 0:
                    raise ValueError(
                        f"Data quality check failed: "
                        f"missing_products={missing_products}, "
                        f"revenue_mismatches={revenue_mismatches}"
                    )

                print("Data quality check passed.")
        finally:
            conn.close()

    orders = generate_orders()
    load_task = load_orders(orders)
    load_task >> check_data_quality()

ecommerce_pipeline()
