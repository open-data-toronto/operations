import json
import pendulum
from airflow.decorators import dag, task, task_group
from utils_operators.slack_operators import task_success_slack_alert, task_failure_slack_alert, GenericSlackOperator
from airflow.operators.python import PythonOperator

def get_data_traditional():
    data = '{"1001": 50, "1002": 100, "1003": 150}'
    return data

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    default_args = {
        'owner': 'Reza',
        'pool' : 'default_pool',
        'priority_weight': 10,
        'on_failure_callback' : task_failure_slack_alert,
        'on_success_callback' : task_success_slack_alert,
        #'trigger_rule': 'none_failed_min_one_success'
        },
    tags=["example"],
)
def taskflow_dag():

    get_data = PythonOperator(
        task_id = "get_data",
        python_callable = get_data_traditional
    )

    @task(pool = 'default_pool' )
    def extract(order):
        # order = '{"1001": 50, "1002": 100, "1003": 150}'
        order_dict = json.loads(order)
        return order_dict


    @task
    def transform_1(orders):
        total_price = 0
        for price in orders.values():
            total_price += price
        return total_price

    @task
    def transform_2(orders):
        total_count = len(orders.keys())
        return total_count


    @task_group(default_args = {
        'trigger_rule': 'none_failed_min_one_success'
        },)
    def transformer_group(orders):
        #return { 'total_price': transform_1(orders), 'total_count': transform_2(orders) }
        return transform_1(orders), transform_2(orders)

    @task
    def load(total_price, total_count):
        print('TOTAL ORDER PRICE = {}'.format(total_price))
        print('TOTAL ORDER COUNT = {}'.format(total_count))
        return {"message" : "Total orders count is {} and total orders price is {}".format(total_count, total_price)}

    slack_whisperer = GenericSlackOperator(
            task_id = "slack_whisperer",
            message_header = "TaskFlow Demo DAG",
            message_body = "",
            #message_content = load(*transformer_group(orders)),
            message_content_task_id = "load",
            message_content_task_key = "message",
        )
    
    orders = extract(get_data.output)
    #load(transform_1(orders), transform_2(orders))
    load(*transformer_group(orders)) >> slack_whisperer

taskflow_dag()


