from datetime import datetime

from airflow.decorators import dag, task

@dag(
    dag_id='ultra_mesh_chaos_v2',
    start_date=datetime(2021, 12, 1),
    schedule=None,
    catchup=False,
    tags=['visual_chaos', 'decorators']
)
def chaotic_mesh():

    # Define a generic task using the decorator
    @task
    def leaf_node(label):
        print(f"Executing {label}")
        return label

    # --- THE LAYERS ---
    # Layer 0: The Single Source
    start = leaf_node(label="START")

    # Layer 1: Expand to 6 nodes
    layer_1 = [leaf_node.override(task_id=f"L1_node_{i}")(f"L1_{i}") for i in range(6)]

    # Layer 2: Expand to 12 nodes (The "Wide Middle")
    layer_2 = [leaf_node.override(task_id=f"L2_node_{i}")(f"L2_{i}") for i in range(12)]

    # Layer 3: Contract back to 6 nodes
    layer_3 = [leaf_node.override(task_id=f"L3_node_{i}")(f"L3_{i}") for i in range(6)]

    # Layer 4: The Final Sink
    end = leaf_node(label="END")

    # --- THE CHAOS (Dependencies) ---
    
    # Connect Start to all of Layer 1
    start >> layer_1

    # CROSS-POLLINATION: Every node in L1 connects to EVERY node in L2
    # This generates 6 * 12 = 72 intersecting lines
    layer_1 >> layer_2

    # Every node in L2 connects to EVERY node in L3
    # This generates 12 * 6 = 72 more intersecting lines
    layer_2 >> layer_3

    # Connect all of Layer 3 to the final End node
    layer_3 >> end

    # THE "GLITCH" LINE: A single line cutting across the entire graph
    start >> end

# Call the DAG function to register it
chaotic_mesh()