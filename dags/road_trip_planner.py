from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import cross_downstream
from datetime import datetime

with DAG(
        dag_id="road_trip_planner",
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["architecture", "routing","thirdDAG"]
) as dag:

    # ------------------------------------------------------------------------
    # 1. INITIALIZATION
    # ------------------------------------------------------------------------
    start = EmptyOperator(task_id="announce_road_trip")
    end = EmptyOperator(task_id="drive_away_exhausted")

    # ------------------------------------------------------------------------
    # 2. TASK DEFINITIONS (The Entities)
    # ------------------------------------------------------------------------

    # Tier 1: The compute resources (Parallel Start)
    passengers = [
        EmptyOperator(task_id="prep_driver_vansh"),
        EmptyOperator(task_id="prep_navigator_deepshe"),
        EmptyOperator(task_id="prep_hero_service_dog")
    ]

    # Tier 2: The Data Extraction Phase
    # Dynamically generating 50 independent tasks to simulate massive parallel extraction
    items = [EmptyOperator(task_id=f"gather_item_{i}") for i in range(1, 51)]

    # Tier 3: The Target Destinations (Specific partitions/tables)
    bags = [
        EmptyOperator(task_id="pack_vanshs_duffel"),
        EmptyOperator(task_id="pack_deepshes_suitcase"),
        EmptyOperator(task_id="pack_heros_gear_bag")
    ]

    # Tier 4: The Final Load Phase
    car_zones = [
        EmptyOperator(task_id="load_trunk"),
        EmptyOperator(task_id="load_roof_box"),
        EmptyOperator(task_id="load_back_seat"),
        EmptyOperator(task_id="load_glovebox")
    ]

    # ------------------------------------------------------------------------
    # 3. ORCHESTRATION & DEPENDENCIES (The Architecture)
    # ------------------------------------------------------------------------

    # Kick off the parallel prep phase
    start >> passengers

    # Logic: ALL 3 passengers must be in a 'success' state before ANY of the 50 items 
    cross_downstream(passengers, items)

    # Logic: Using modulo (i % 3) to evenly distribute the 50 items across the 3 bags. 
    for i, item in enumerate(items):
        item >> bags[i % 3]

        # THE FINAL CHECKPOINT
    cross_downstream(bags, car_zones)

    # Fan-in all parallel loading tasks to the final end state
    for zone in car_zones:
        zone >> end