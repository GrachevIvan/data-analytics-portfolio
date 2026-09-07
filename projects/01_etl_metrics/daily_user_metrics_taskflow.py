from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandahouse as ph
import pandas as pd
import os

# ВАЖНО: Данные для подключения к ClickHouse удалены в соответствии с условиями использования учебных материалов.
# Для запуска необходимо настроить собственное подключение через переменные окружения или напрямую.
SOURCE_CONN = {
    'host': os.getenv('CLICKHOUSE_SOURCE_HOST', 'your_host'),
    'password': os.getenv('CLICKHOUSE_SOURCE_PASSWORD', 'your_password'),
    'user': os.getenv('CLICKHOUSE_SOURCE_USER', 'your_user'),
    'database': os.getenv('CLICKHOUSE_SOURCE_DB', 'your_database')
}

TARGET_CONN = {
    'host': os.getenv('CLICKHOUSE_TARGET_HOST', 'your_host'),
    'password': os.getenv('CLICKHOUSE_TARGET_PASSWORD', 'your_password'),
    'user': os.getenv('CLICKHOUSE_TARGET_USER', 'your_user'),
    'database': os.getenv('CLICKHOUSE_TARGET_DB', 'your_database')
}

# Имя таблицы с префиксом через подчёркивание
TARGET_TABLE = 'grachev_student_daily_user_metrics'

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id='daily_user_metrics_taskflow',
    default_args=default_args,
    description='Ежедневный расчёт метрик и запись в test',
    schedule_interval='@daily',
    start_date=datetime(2026, 8, 12),
    catchup=False,
    max_active_runs=1
)
def etl_dag():

    @task
    def extract_feed_actions():
        ds = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        query = f"""
        SELECT user_id,
               sumIf(1, action = 'view') as views,
               sumIf(1, action = 'like') as likes,
               any(gender) as gender,
               any(age) as age,
               any(os) as os
        FROM simulator_20260720.feed_actions
        WHERE toDate(time) = '{ds}'
        GROUP BY user_id
        """
        return ph.read_clickhouse(query, connection=SOURCE_CONN)

    @task
    def extract_message_actions():
        ds = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        query = f"""
        SELECT user_id,
               sum(msgs_sent) as messages_sent,
               sum(msgs_received) as messages_received,
               sum(users_sent) as users_sent,
               sum(users_received) as users_received,
               any(gender) as gender,
               any(age) as age,
               any(os) as os
        FROM (
            SELECT user_id,
                   any(gender) as gender,
                   any(age) as age,
                   any(os) as os,
                   count() as msgs_sent,
                   uniq(receiver_id) as users_sent,
                   0 as msgs_received,
                   0 as users_received
            FROM simulator_20260720.message_actions
            WHERE toDate(time) = '{ds}'
            GROUP BY user_id

            UNION ALL

            SELECT receiver_id as user_id,
                   any(gender) as gender,
                   any(age) as age,
                   any(os) as os,
                   0 as msgs_sent,
                   0 as users_sent,
                   count() as msgs_received,
                   uniq(user_id) as users_received
            FROM simulator_20260720.message_actions
            WHERE toDate(time) = '{ds}'
            GROUP BY receiver_id
        )
        GROUP BY user_id
        """
        return ph.read_clickhouse(query, connection=SOURCE_CONN)

    @task
    def merge_tables(feed_df, message_df):
        merged = pd.merge(feed_df, message_df, on='user_id', how='outer')
        num_cols = ['views', 'likes', 'messages_sent', 'messages_received', 'users_sent', 'users_received']
        for col in num_cols:
            merged[col] = merged[col].fillna(0).astype('int64')
        for col in ['gender', 'age', 'os']:
            if col + '_x' in merged.columns and col + '_y' in merged.columns:
                merged[col] = merged[col + '_x'].fillna(merged[col + '_y'])
                merged.drop([col + '_x', col + '_y'], axis=1, inplace=True)
            elif col + '_x' in merged.columns:
                merged[col] = merged[col + '_x']
                merged.drop([col + '_x'], axis=1, inplace=True)
            elif col + '_y' in merged.columns:
                merged[col] = merged[col + '_y']
                merged.drop([col + '_y'], axis=1, inplace=True)
        return merged

    @task
    def calc_os_metrics(merged):
        agg = merged.groupby('os', as_index=False).agg({
            'views': 'sum', 'likes': 'sum', 'messages_received': 'sum',
            'messages_sent': 'sum', 'users_received': 'sum', 'users_sent': 'sum'
        })
        agg['dimension'] = 'os'
        agg.rename(columns={'os': 'dimension_value'}, inplace=True)
        return agg[['dimension', 'dimension_value', 'views', 'likes',
                    'messages_received', 'messages_sent', 'users_received', 'users_sent']]

    @task
    def calc_gender_metrics(merged):
        agg = merged.groupby('gender', as_index=False).agg({
            'views': 'sum', 'likes': 'sum', 'messages_received': 'sum',
            'messages_sent': 'sum', 'users_received': 'sum', 'users_sent': 'sum'
        })
        agg['dimension'] = 'gender'
        agg.rename(columns={'gender': 'dimension_value'}, inplace=True)
        return agg[['dimension', 'dimension_value', 'views', 'likes',
                    'messages_received', 'messages_sent', 'users_received', 'users_sent']]

    @task
    def calc_age_metrics(merged):
        agg = merged.groupby('age', as_index=False).agg({
            'views': 'sum', 'likes': 'sum', 'messages_received': 'sum',
            'messages_sent': 'sum', 'users_received': 'sum', 'users_sent': 'sum'
        })
        agg['dimension'] = 'age'
        agg.rename(columns={'age': 'dimension_value'}, inplace=True)
        return agg[['dimension', 'dimension_value', 'views', 'likes',
                    'messages_received', 'messages_sent', 'users_received', 'users_sent']]

    @task
    def load_to_clickhouse(os_df, gender_df, age_df):
        ds = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        final_df = pd.concat([os_df, gender_df, age_df], ignore_index=True)
        final_df['event_date'] = pd.to_datetime(ds).date()
        final_df['dimension_value'] = final_df['dimension_value'].astype(str)

        table_name = TARGET_TABLE  # 'grachev_student_daily_user_metrics'

        create_query = f"""
        CREATE TABLE IF NOT EXISTS test.{table_name} (
            event_date Date,
            dimension String,
            dimension_value String,
            views UInt64,
            likes UInt64,
            messages_received UInt64,
            messages_sent UInt64,
            users_received UInt64,
            users_sent UInt64
        ) ENGINE = MergeTree()
        ORDER BY (event_date, dimension, dimension_value)
        """
        ph.execute(create_query, connection=TARGET_CONN)

        delete_query = f"ALTER TABLE test.{table_name} DELETE WHERE event_date = '{ds}'"
        ph.execute(delete_query, connection=TARGET_CONN)

        ph.to_clickhouse(final_df, table=table_name, connection=TARGET_CONN, index=False)

    feed_df = extract_feed_actions()
    message_df = extract_message_actions()
    merged = merge_tables(feed_df, message_df)

    os_metrics = calc_os_metrics(merged)
    gender_metrics = calc_gender_metrics(merged)
    age_metrics = calc_age_metrics(merged)

    load_to_clickhouse(os_metrics, gender_metrics, age_metrics)

etl_dag = etl_dag()
