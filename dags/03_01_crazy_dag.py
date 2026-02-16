from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.baseoperator import cross_downstream

@dag(
    dag_id='fixed_neural_chaos_v3',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['visual_chaos', 'fixed_logic']
)
def chaotic_mesh():

    @task
    def leaf_node(label):
        return label

    # --- THE LAYERS ---
    start = leaf_node.override(task_id="START")("start")
    
    # Layer 1: 6 nodes
    l1 = [leaf_node.override(task_id=f"L1_N{i}")(f"L1_{i}") for i in range(6)]
    
    # Layer 2: 12 nodes (The "Wide Middle")
    l2 = [leaf_node.override(task_id=f"L2_N{i}")(f"L2_{i}") for i in range(12)]
    
    # Layer 3: 6 nodes
    l3 = [leaf_node.override(task_id=f"L3_N{i}")(f"L3_{i}") for i in range(6)]
    
    end = leaf_node.override(task_id="END")("end")

    # --- THE CHAOS (Using cross_downstream for lists) ---
    
    # 1. Connect Start (single) to Layer 1 (list)
    start >> l1

    # 2. CROSS-POLLINATION: Connect every L1 node to every L2 node
    # This is what was causing your error!
    cross_downstream(l1, l2)

    # 3. Connect every L2 node to every L3 node
    cross_downstream(l2, l3)

    # 4. Connect Layer 3 (list) to End (single)
    l3 >> end

    # 5. THE "LONG JUMP" (One line spanning the whole graph)
    start >> end

chaotic_mesh()