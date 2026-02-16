"""
4. Create your own crazy DAG that results in the most complex looking “Graph View” representation of the DAG as possible. The more lines, dependencies, etc, the better! Feel free to have fun with this one.

This DAG contains 26 tasks:
- start
- layer 1
  - 6 tasks
    - L1_N0 to L1_N5
- layer 2
  - 12 tasks
    - L2_N0 to L2_N11
- layer 3
  - 6 tasks
    - L3_N0 to L3_N5
- end

Schedule - No schedule, therefore, we should trigger this DAG manually

Start_date - December 1, 2021

Catchup - False
  - The scheduler will only run the most recent interval

"""

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.baseoperator import cross_downstream

@dag(
    dag_id='crazy_graph_dag',
    start_date=datetime(2021, 12, 1),
    schedule=None,
    catchup=False,
    tags=['crazy', 'graph_demo']
)
def crazy_graph_dag():

    @task
    def leaf_node(label):
        return label

    start = leaf_node.override(task_id="START")("start")
    
    # Layer 1: 6 nodes 
    l1 = [leaf_node.override(task_id=f"L1_N{i}")(f"L1_{i}") for i in range(6)]
    
    # Layer 2: 12 nodes 
    l2 = [leaf_node.override(task_id=f"L2_N{i}")(f"L2_{i}") for i in range(12)]
    
    # Layer 3: 6 nodes
    l3 = [leaf_node.override(task_id=f"L3_N{i}")(f"L3_{i}") for i in range(6)]
    
    # End
    end = leaf_node.override(task_id="END")("end")
    
    # Start connect to all the L1 nodes
    start >> l1

    # Connect every L1 node to every L2 node
    cross_downstream(l1, l2)

    # Connect every L2 node to every L3 node
    cross_downstream(l2, l3)

    # Connect Layer 3 to End 
    l3 >> end

    # Connect the start to the end
    start >> end

crazy_graph_dag()