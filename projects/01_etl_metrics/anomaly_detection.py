# Импорт библиотек
from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
import pandahouse as ph
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import telegram

# Telegram токен и ID чата удалены в соответствии с условиями использования учебных материалов.
TOKEN = os.getenv('TELEGRAM_TOKEN', 'your_token')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', 'your_chat_id')

# ---------- Часовой пояс Москвы ----------
MSK = timezone(timedelta(hours=3))

# ---------- Подключение к ClickHouse ----------
SOURCE_CONN = {
    'host': os.getenv('CLICKHOUSE_SOURCE_HOST', 'your_host'),
    'password': os.getenv('CLICKHOUSE_SOURCE_PASSWORD', 'your_password'),
    'user': os.getenv('CLICKHOUSE_SOURCE_USER', 'your_user'),
    'database': os.getenv('CLICKHOUSE_SOURCE_DB', 'your_database')
}

# ---------- Настройки ----------
HISTORY_DAYS = 7          # период для расчёта статистик
RECENT_HOURS = 6          # последние часы для графика
Z_SCORE = 1.5             # коэффициент для доверительного интервала
INTERVAL_MIN = 15         # длина интервала в минутах

default_args = {
    'owner': 'grachev_ivan',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id='anomaly_detection_final',
    default_args=default_args,
    description='Система алертов со статистическими доверительными интервалами',
    schedule_interval='*/15 * * * *',       # каждые 15 минут
    start_date=datetime(2026, 8, 30),       # дата начала (сегодня)
    catchup=False,
    max_active_runs=1
)
def anomaly_dag():

    @task
    def check_metrics_and_alert():
        # Текущее московское время, округляем вниз до 15 минут – последний завершённый интервал
        now_msk = datetime.now(MSK)
        rounded = now_msk - timedelta(minutes=now_msk.minute % INTERVAL_MIN,
                                      seconds=now_msk.second,
                                      microseconds=now_msk.microsecond)
        end_msk = rounded
        start_msk = end_msk - timedelta(minutes=INTERVAL_MIN)

        # Периоды для запросов (московское время передаём с таймзоной в ClickHouse)
        history_start = (end_msk - timedelta(days=HISTORY_DAYS)).strftime('%Y-%m-%d %H:%M:%S')
        history_end = end_msk.strftime('%Y-%m-%d %H:%M:%S')
        recent_start = (end_msk - timedelta(hours=RECENT_HOURS)).strftime('%Y-%m-%d %H:%M:%S')
        recent_end = end_msk.strftime('%Y-%m-%d %H:%M:%S')

        # ========== Загрузка истории (7 дней) для статистик ==========
        # Лента
        query_feed_hist = f"""
        SELECT
            toString(toTimezone(toStartOfInterval(time, INTERVAL {INTERVAL_MIN} MINUTE), 'Europe/Moscow')) AS interval_start,
            uniqExact(user_id) AS active_users_feed,
            sumIf(1, action = 'view') AS views,
            sumIf(1, action = 'like') AS likes,
            if(views > 0, likes / views, 0) AS ctr
        FROM simulator_20260720.feed_actions
        WHERE toDateTime(time) >= toDateTime('{history_start}', 'Europe/Moscow')
          AND toDateTime(time) < toDateTime('{history_end}', 'Europe/Moscow')
        GROUP BY interval_start
        ORDER BY interval_start
        """
        feed_hist = ph.read_clickhouse(query_feed_hist, connection=SOURCE_CONN)
        feed_hist['interval_start'] = pd.to_datetime(feed_hist['interval_start'])
        feed_hist['time_of_day'] = feed_hist['interval_start'].dt.strftime('%H:%M')

        # Мессенджер: отправленные сообщения
        query_msg_sent_hist = f"""
        SELECT
            toString(toTimezone(toStartOfInterval(time, INTERVAL {INTERVAL_MIN} MINUTE), 'Europe/Moscow')) AS interval_start,
            count() AS messages_sent
        FROM simulator_20260720.message_actions
        WHERE toDateTime(time) >= toDateTime('{history_start}', 'Europe/Moscow')
          AND toDateTime(time) < toDateTime('{history_end}', 'Europe/Moscow')
        GROUP BY interval_start
        ORDER BY interval_start
        """
        msg_sent_hist = ph.read_clickhouse(query_msg_sent_hist, connection=SOURCE_CONN)
        msg_sent_hist['interval_start'] = pd.to_datetime(msg_sent_hist['interval_start'])
        msg_sent_hist['time_of_day'] = msg_sent_hist['interval_start'].dt.strftime('%H:%M')

        # Мессенджер: активные пользователи (отправители и получатели)
        query_msg_active_hist = f"""
        SELECT
            interval_start,
            uniqExact(user_id) AS active_users_messenger
        FROM (
            SELECT
                toString(toTimezone(toStartOfInterval(time, INTERVAL {INTERVAL_MIN} MINUTE), 'Europe/Moscow')) AS interval_start,
                user_id
            FROM simulator_20260720.message_actions
            WHERE toDateTime(time) >= toDateTime('{history_start}', 'Europe/Moscow')
              AND toDateTime(time) < toDateTime('{history_end}', 'Europe/Moscow')
            UNION ALL
            SELECT
                toString(toTimezone(toStartOfInterval(time, INTERVAL {INTERVAL_MIN} MINUTE), 'Europe/Moscow')) AS interval_start,
                receiver_id AS user_id
            FROM simulator_20260720.message_actions
            WHERE toDateTime(time) >= toDateTime('{history_start}', 'Europe/Moscow')
              AND toDateTime(time) < toDateTime('{history_end}', 'Europe/Moscow')
        )
        GROUP BY interval_start
        ORDER BY interval_start
        """
        msg_active_hist = ph.read_clickhouse(query_msg_active_hist, connection=SOURCE_CONN)
        msg_active_hist['interval_start'] = pd.to_datetime(msg_active_hist['interval_start'])
        msg_active_hist['time_of_day'] = msg_active_hist['interval_start'].dt.strftime('%H:%M')

        # Объединяем мессенджер
        msg_hist = pd.merge(msg_sent_hist, msg_active_hist, on='interval_start', how='outer').fillna(0)
        msg_hist['interval_start'] = pd.to_datetime(msg_hist['interval_start'])
        msg_hist['time_of_day'] = msg_hist['interval_start'].dt.strftime('%H:%M')

        # ========== Загрузка последних 6 часов (для графиков) ==========
        query_feed_recent = f"""
        SELECT
            toString(toTimezone(toStartOfInterval(time, INTERVAL {INTERVAL_MIN} MINUTE), 'Europe/Moscow')) AS interval_start,
            uniqExact(user_id) AS active_users_feed,
            sumIf(1, action = 'view') AS views,
            sumIf(1, action = 'like') AS likes,
            if(views > 0, likes / views, 0) AS ctr
        FROM simulator_20260720.feed_actions
        WHERE toDateTime(time) >= toDateTime('{recent_start}', 'Europe/Moscow')
          AND toDateTime(time) < toDateTime('{recent_end}', 'Europe/Moscow')
        GROUP BY interval_start
        ORDER BY interval_start
        """
        feed_recent = ph.read_clickhouse(query_feed_recent, connection=SOURCE_CONN)
        feed_recent['interval_start'] = pd.to_datetime(feed_recent['interval_start'])
        feed_recent['time_of_day'] = feed_recent['interval_start'].dt.strftime('%H:%M')

        query_msg_sent_recent = f"""
        SELECT
            toString(toTimezone(toStartOfInterval(time, INTERVAL {INTERVAL_MIN} MINUTE), 'Europe/Moscow')) AS interval_start,
            count() AS messages_sent
        FROM simulator_20260720.message_actions
        WHERE toDateTime(time) >= toDateTime('{recent_start}', 'Europe/Moscow')
          AND toDateTime(time) < toDateTime('{recent_end}', 'Europe/Moscow')
        GROUP BY interval_start
        ORDER BY interval_start
        """
        msg_sent_recent = ph.read_clickhouse(query_msg_sent_recent, connection=SOURCE_CONN)
        msg_sent_recent['interval_start'] = pd.to_datetime(msg_sent_recent['interval_start'])
        msg_sent_recent['time_of_day'] = msg_sent_recent['interval_start'].dt.strftime('%H:%M')

        query_msg_active_recent = f"""
        SELECT
            interval_start,
            uniqExact(user_id) AS active_users_messenger
        FROM (
            SELECT
                toString(toTimezone(toStartOfInterval(time, INTERVAL {INTERVAL_MIN} MINUTE), 'Europe/Moscow')) AS interval_start,
                user_id
            FROM simulator_20260720.message_actions
            WHERE toDateTime(time) >= toDateTime('{recent_start}', 'Europe/Moscow')
              AND toDateTime(time) < toDateTime('{recent_end}', 'Europe/Moscow')
            UNION ALL
            SELECT
                toString(toTimezone(toStartOfInterval(time, INTERVAL {INTERVAL_MIN} MINUTE), 'Europe/Moscow')) AS interval_start,
                receiver_id AS user_id
            FROM simulator_20260720.message_actions
            WHERE toDateTime(time) >= toDateTime('{recent_start}', 'Europe/Moscow')
              AND toDateTime(time) < toDateTime('{recent_end}', 'Europe/Moscow')
        )
        GROUP BY interval_start
        ORDER BY interval_start
        """
        msg_active_recent = ph.read_clickhouse(query_msg_active_recent, connection=SOURCE_CONN)
        msg_active_recent['interval_start'] = pd.to_datetime(msg_active_recent['interval_start'])
        msg_active_recent['time_of_day'] = msg_active_recent['interval_start'].dt.strftime('%H:%M')

        msg_recent = pd.merge(msg_sent_recent, msg_active_recent, on='interval_start', how='outer').fillna(0)
        msg_recent['interval_start'] = pd.to_datetime(msg_recent['interval_start'])
        msg_recent['time_of_day'] = msg_recent['interval_start'].dt.strftime('%H:%M')

        # ========== Расчёт статистик по времени суток ==========
        metrics_feed = ['active_users_feed', 'views', 'likes', 'ctr']
        metrics_msg = ['active_users_messenger', 'messages_sent']
        stats = {}

        for metric in metrics_feed:
            grouped = feed_hist.groupby('time_of_day')[metric].agg(['mean', 'std']).reset_index()
            grouped.columns = ['time_of_day', 'mean', 'std']
            stats[metric] = grouped

        for metric in metrics_msg:
            grouped = msg_hist.groupby('time_of_day')[metric].agg(['mean', 'std']).reset_index()
            grouped.columns = ['time_of_day', 'mean', 'std']
            stats[metric] = grouped

        # ========== Фактические значения для последнего интервала ==========
        current_time_str = start_msk.strftime('%H:%M')
        s_str = start_msk.strftime('%Y-%m-%d %H:%M:%S')
        e_str = end_msk.strftime('%Y-%m-%d %H:%M:%S')

        # Лента
        query_feed_fact = f"""
        SELECT
            uniqExact(user_id) AS active_users_feed,
            sumIf(1, action = 'view') AS views,
            sumIf(1, action = 'like') AS likes,
            if(views > 0, likes / views, 0) AS ctr
        FROM simulator_20260720.feed_actions
        WHERE toDateTime(time) >= toDateTime('{s_str}', 'Europe/Moscow')
          AND toDateTime(time) < toDateTime('{e_str}', 'Europe/Moscow')
        """
        feed_fact = ph.read_clickhouse(query_feed_fact, connection=SOURCE_CONN).iloc[0]

        # Мессенджер
        query_msg_sent_fact = f"""
        SELECT count() AS messages_sent
        FROM simulator_20260720.message_actions
        WHERE toDateTime(time) >= toDateTime('{s_str}', 'Europe/Moscow')
          AND toDateTime(time) < toDateTime('{e_str}', 'Europe/Moscow')
        """
        msg_sent_fact = ph.read_clickhouse(query_msg_sent_fact, connection=SOURCE_CONN).iloc[0]['messages_sent']

        query_msg_active_fact = f"""
        SELECT uniqExact(user_id) AS active_users_messenger
        FROM (
            SELECT user_id FROM simulator_20260720.message_actions
            WHERE toDateTime(time) >= toDateTime('{s_str}', 'Europe/Moscow')
              AND toDateTime(time) < toDateTime('{e_str}', 'Europe/Moscow')
            UNION ALL
            SELECT receiver_id AS user_id FROM simulator_20260720.message_actions
            WHERE toDateTime(time) >= toDateTime('{s_str}', 'Europe/Moscow')
              AND toDateTime(time) < toDateTime('{e_str}', 'Europe/Moscow')
        )
        """
        msg_active_fact = ph.read_clickhouse(query_msg_active_fact, connection=SOURCE_CONN).iloc[0]['active_users_messenger']

        facts = {
            'active_users_feed': float(feed_fact['active_users_feed']),
            'views': float(feed_fact['views']),
            'likes': float(feed_fact['likes']),
            'ctr': float(feed_fact['ctr']),
            'active_users_messenger': float(msg_active_fact),
            'messages_sent': float(msg_sent_fact)
        }

        # ========== Проверка каждой метрики и отправка алертов ==========
        for metric in metrics_feed + metrics_msg:
            stat_row = stats[metric][stats[metric]['time_of_day'] == current_time_str]
            if stat_row.empty:
                continue
            mean_val = float(stat_row.iloc[0]['mean'])
            std_val = float(stat_row.iloc[0]['std']) if not pd.isna(stat_row.iloc[0]['std']) else 0.0
            lower = mean_val - Z_SCORE * std_val
            upper = mean_val + Z_SCORE * std_val
            fact = facts[metric]

            if fact < lower or fact > upper:
                # Строим график
                fig, ax = plt.subplots(figsize=(12, 5))
                # Данные за последние 6 часов для этой метрики
                if metric in metrics_feed:
                    recent_series = feed_recent.set_index('interval_start')[metric]
                else:
                    recent_series = msg_recent.set_index('interval_start')[metric]

                # Среднее и доверительный интервал для каждого интервала recent
                mean_series = []
                lower_series = []
                upper_series = []
                for t in recent_series.index:
                    t_str = t.strftime('%H:%M')
                    row = stats[metric][stats[metric]['time_of_day'] == t_str]
                    if not row.empty:
                        m = row.iloc[0]['mean']
                        s = row.iloc[0]['std'] if not pd.isna(row.iloc[0]['std']) else 0
                        mean_series.append(m)
                        lower_series.append(m - Z_SCORE * s)
                        upper_series.append(m + Z_SCORE * s)
                    else:
                        mean_series.append(np.nan)
                        lower_series.append(np.nan)
                        upper_series.append(np.nan)

                ax.plot(recent_series.index, recent_series.values, marker='o', color='blue', label='Факт')
                ax.plot(recent_series.index, mean_series, color='orange', linestyle='--', label='Среднее')
                ax.fill_between(recent_series.index, lower_series, upper_series,
                                alpha=0.2, color='green', label='Доверительный интервал (±{}σ)'.format(Z_SCORE))
                # Точка аномалии
                ax.scatter([start_msk.replace(tzinfo=None)], [fact], color='red', s=100, zorder=5, label='Аномалия')

                ax.set_title('Метрика: {} (лента/мессенджер)'.format(metric))
                ax.set_xlabel('Время (МСК)')
                ax.set_ylabel('Значение')
                ax.legend()
                ax.grid(True, linestyle='--', alpha=0.7)
                plt.xticks(rotation=45)
                plt.tight_layout()

                buf = io.BytesIO()
                plt.savefig(buf, format='png')
                buf.seek(0)
                plt.close(fig)

                # Текст алерта
                deviation_pct = abs((fact - mean_val) / mean_val) * 100 if mean_val != 0 else 0.0
                interval_str = f"{start_msk.strftime('%H:%M')}–{end_msk.strftime('%H:%M')} МСК"
                alert_text = (
                    f"🚨 Аномалия в метрике {metric}\n"
                    f"Срез: лента/мессенджер\n"
                    f"Интервал: {interval_str}\n"
                    f"Текущее значение: {fact:.2f}\n"
                    f"Ожидаемый диапазон: [{lower:.2f}, {upper:.2f}]\n"
                    f"Отклонение от среднего: {deviation_pct:.1f}%"
                )

                # Отправка
                bot = telegram.Bot(token=TOKEN)
                try:
                    bot.send_photo(chat_id=CHAT_ID, photo=buf, caption=alert_text)
                    print(f"Алерт отправлен для {metric}. Значение {fact:.2f} вне [{lower:.2f}, {upper:.2f}]")
                except Exception as e:
                    print(f"Ошибка отправки алерта для {metric}: {e}")
                finally:
                    buf.close()

    check_metrics_and_alert()

anomaly_dag = anomaly_dag()
