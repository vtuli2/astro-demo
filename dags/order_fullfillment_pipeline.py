"""
order_fulfillment_pipeline.py
"""

from __future__ import annotations
from datetime import datetime, timedelta
import random
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

default_args = {
    "owner": "fulfillment-ops",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="order_fulfillment_pipeline",
    description="Validate incoming orders and route to the correct fulfillment path",
    schedule="*/30 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["shopsmart", "fulfillment", "complex", "branching"],
    # Only one run at a time. Fulfillment writes to shared warehouse queues,
    # so overlapping runs risk double-picking inventory.
    max_active_runs=1,
    doc_md=__doc__,
)
def order_fulfillment_pipeline():
    """
    Pipeline shape (the "crazy" graph view):
                              ┌── validate_inventory ──┐
                              ├── validate_address ────┤
        receive_order_batch ──┼── validate_payment ────┼── consolidate_validations ── route_by_shipping_type ──┬── [domestic_standard: 3 tasks] ──┐
                              └── validate_customer ───┘                                                       ├── [domestic_express: 3 tasks]  ──┤
                                                                                                               ├── [international: 3 tasks]     ──┼── update_fulfillment_status ── notify_warehouse_ops                                                                                               └── [hold_for_review: 1 task]    ──┘
    """

    receive_order_batch = EmptyOperator(task_id="receive_order_batch")

    @task
    def validate_inventory() -> dict:
        """Confirm all line items are in stock and reservable."""
        all_in_stock = random.random() > 0.1
        print(f"Inventory check: all_in_stock={all_in_stock}")
        return {"check": "inventory", "passed": all_in_stock}

    @task
    def validate_address() -> dict:
        """Confirm shipping address is deliverable and determine if international."""
        # Simulate: 70% domestic, 25% international, 5% undeliverable.
        roll = random.random()
        if roll < 0.05:
            result = {"check": "address", "passed": False, "region": None}
        elif roll < 0.30:
            result = {"check": "address", "passed": True, "region": "international"}
        else:
            result = {"check": "address", "passed": True, "region": "domestic"}
        print(f"Address check: {result}")
        return result

    @task
    def validate_payment() -> dict:
        """Confirm payment auth hold is still valid."""
        # Simulate: 95% of payments validate cleanly.
        auth_valid = random.random() > 0.05
        print(f"Payment check: auth_valid={auth_valid}")
        return {"check": "payment", "passed": auth_valid}

    @task
    def validate_customer() -> dict:
        """Confirm customer account is in good standing (no blocks, no flags)."""
        # Simulate: 98% of customers pass; occasional holds for account issues.
        in_good_standing = random.random() > 0.02
        print(f"Customer check: in_good_standing={in_good_standing}")
        return {"check": "customer", "passed": in_good_standing}

    @task
    def consolidate_validations(
            inventory: dict, address: dict, payment: dict, customer: dict
    ) -> dict:
        all_passed = all(r["passed"] for r in [inventory, address, payment, customer])
        region = address.get("region")  # 'domestic', 'international', or None

        # Shipping speed would normally come from the order. Randomized here
        # for demo variety.
        shipping_speed = random.choice(["standard", "express"])

        result = {
            "all_validations_passed": all_passed,
            "region": region,
            "shipping_speed": shipping_speed,
            "failed_checks": [r["check"] for r in [inventory, address, payment, customer] if not r["passed"]],
        }
        print(f"Consolidation result: {result}")
        return result

    # The decision tree is simple:
    #   - Any validation failed -> hold for manual review
    #   - International -> international path (customs, export docs)
    #   - Domestic + express -> express path (overnight carrier)
    #   - Domestic + standard -> standard path (ground carrier)
    def _route_by_shipping_type(ti=None, **context) -> str:
        result = ti.xcom_pull(task_ids="consolidate_validations")

        if not result["all_validations_passed"]:
            branch = "hold_for_review"
        elif result["region"] == "international":
            branch = "generate_customs_docs"       # first task of international path
        elif result["shipping_speed"] == "express":
            branch = "assign_to_express_warehouse"  # first task of express path
        else:
            branch = "assign_to_nearest_warehouse"  # first task of standard path

        print(f"Routing to branch: {branch}")
        return branch

    route_by_shipping_type = BranchPythonOperator(
        task_id="route_by_shipping_type",
        python_callable=_route_by_shipping_type,
    )


    # --- Standard domestic branch (ground carrier, nearest warehouse) ---
    assign_to_nearest_warehouse = EmptyOperator(task_id="assign_to_nearest_warehouse")
    generate_ground_label = EmptyOperator(task_id="generate_ground_label")
    notify_standard_team = EmptyOperator(task_id="notify_standard_team")

    # --- Express domestic branch (overnight carrier, express-capable warehouse) ---
    assign_to_express_warehouse = EmptyOperator(task_id="assign_to_express_warehouse")
    generate_overnight_label = EmptyOperator(task_id="generate_overnight_label")
    notify_express_team = EmptyOperator(task_id="notify_express_team")

    # --- International branch (customs paperwork, international carrier) ---
    generate_customs_docs = EmptyOperator(task_id="generate_customs_docs")
    generate_intl_label = EmptyOperator(task_id="generate_intl_label")
    notify_intl_team = EmptyOperator(task_id="notify_intl_team")

    # --- Hold-for-review branch (validation failed, don't ship) ---
    hold_for_review = EmptyOperator(task_id="hold_for_review")

    update_fulfillment_status = EmptyOperator(
        task_id="update_fulfillment_status",
        trigger_rule="none_failed_min_one_success",
    )

    notify_warehouse_ops = EmptyOperator(task_id="notify_warehouse_ops")

    # Wire the full graph
    inventory = validate_inventory()
    address = validate_address()
    payment = validate_payment()
    customer = validate_customer()

    receive_order_batch >> [inventory, address, payment, customer]

    consolidated = consolidate_validations(inventory, address, payment, customer)

    consolidated >> route_by_shipping_type

    route_by_shipping_type >> [
        assign_to_nearest_warehouse,
        assign_to_express_warehouse,
        generate_customs_docs,
        hold_for_review,
    ]

    assign_to_nearest_warehouse >> generate_ground_label >> notify_standard_team
    assign_to_express_warehouse >> generate_overnight_label >> notify_express_team
    generate_customs_docs >> generate_intl_label >> notify_intl_team

    [
        notify_standard_team,
        notify_express_team,
        notify_intl_team,
        hold_for_review,
    ] >> update_fulfillment_status >> notify_warehouse_ops


order_fulfillment_pipeline()
