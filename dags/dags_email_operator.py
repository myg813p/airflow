from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dag_bash_select_fruit",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2024, 2, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    send_email_task = EmailOperator(
        task_id="send_email_task",
        to="myg813p@naver.com",
        subject="Airflow 성공메일",
        html_content="Airflow 작업이 완료됐습니다.",
    )