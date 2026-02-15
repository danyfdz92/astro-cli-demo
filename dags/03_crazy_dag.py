"""
Crazy DAG: Maximizes Graph View complexity—lots of branches, joins, crossing lines (TaskFlow API).
"""
from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="crazy_graph_dag",
    schedule=None,
    start_date=datetime(2021, 12, 1),
    catchup=False,
    tags=["crazy", "graph-demo"],
)
def crazy_graph_dag():
    @task
    def start():
        pass

    @task
    def branch_a1():
        pass

    @task
    def branch_b1():
        pass

    @task
    def branch_c1():
        pass

    @task
    def a2a():
        pass

    @task
    def a2b():
        pass

    @task
    def b2a():
        pass

    @task
    def b2b():
        pass

    @task
    def c2a():
        pass

    @task
    def c2b():
        pass

    @task
    def mid_converge():
        pass

    @task
    def path_p():
        pass

    @task
    def path_q():
        pass

    @task
    def path_r():
        pass

    @task
    def end():
        pass

    # Entry layer
    start_task = start()

    # Branch into 3
    a1_task = branch_a1()
    b1_task = branch_b1()
    c1_task = branch_c1()
    start_task >> [a1_task, b1_task, c1_task]

    # Each branch fans out to 2
    a2a_t = a2a()
    a2b_t = a2b()
    b2a_t = b2a()
    b2b_t = b2b()
    c2a_t = c2a()
    c2b_t = c2b()
    a1_task >> [a2a_t, a2b_t]
    b1_task >> [b2a_t, b2b_t]
    c1_task >> [c2a_t, c2b_t]

    # Cross-connections
    a2a_t >> b2a_t
    a2a_t >> c2a_t
    b2a_t >> a2b_t
    c2b_t >> b2b_t

    # Mid-layer convergence
    mid_t = mid_converge()
    [a2b_t, b2b_t, c2b_t] >> mid_t

    # Branch again from mid
    p_t = path_p()
    q_t = path_q()
    r_t = path_r()
    mid_t >> [p_t, q_t, r_t]
    p_t >> q_t
    p_t >> r_t
    q_t >> r_t

    # Final fan-in
    end_t = end()
    [p_t, q_t, r_t] >> end_t


crazy_graph_dag()
