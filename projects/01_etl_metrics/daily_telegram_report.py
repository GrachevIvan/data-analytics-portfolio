# Импорт декораторов для создания DAG и задач
from airflow.decorators import dag, task
# Импорт даты и времени
from datetime import datetime, timedelta
# Импорт pandahouse для работы с ClickHouse
import pandahouse as ph
# Импорт pandas для обработки данных
import pandas as pd
# Импорт matplotlib для построения графиков
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
# Импорт io для работы с байтовым потоком (сохраняем график в переменную)
import io

# ===== Блок Telegram (закомментирован, так как Telegram недоступен) =====
# import telegram
# TOKEN = '8818975193:AAGK8xef2hDSL2kVWoTnfDO7pDnU4smKSe4'
# CHAT_ID = -1002614297220

# ---------- Подключение к ClickHouse (источник данных) ----------
SOURCE_CONN = {
    'host': 'http://clickhouse.lab.karpov.courses:8123',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20260720'
}

# ---------- Аргументы по умолчанию для DAG ----------
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# ---------- Описание DAG ----------
@dag(
    dag_id='daily_telegram_report',
    default_args=default_args,
    description='Ежедневный отчёт по ленте новостей (печать вместо Telegram)',
    schedule_interval='0 11 * * *',          # каждый день в 11:00
    start_date=datetime(2026, 8, 12),        # дата начала планирования
    catchup=False,                           # не навёрстывать пропущенные запуски
    max_active_runs=1                        # не запускать параллельно
)
def report_dag():

    # ===== Задача 1: Получить метрики за вчерашний день =====
    @task
    def get_yesterday_metrics():
        """
        Собирает DAU, просмотры, лайки и CTR за вчерашний день.
        Возвращает строку с текстом для отчёта.
        """
        yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        query = f"""
        SELECT
            uniqExact(user_id) AS dau,
            sumIf(1, action = 'view') AS views,
            sumIf(1, action = 'like') AS likes,
            likes / views AS ctr
        FROM simulator_20260720.feed_actions
        WHERE toDate(time) = '{yesterday}'
        """
        df = ph.read_clickhouse(query, connection=SOURCE_CONN)
        dau = int(df['dau'].iloc[0])
        views = int(df['views'].iloc[0])
        likes = int(df['likes'].iloc[0])
        ctr = float(df['ctr'].iloc[0])

        report_text = (
            f"📊 Отчёт по ленте новостей за {yesterday}\n\n"
            f"👥 DAU: {dau}\n"
            f"👀 Просмотры: {views}\n"
            f"❤️ Лайки: {likes}\n"
            f"🔘 CTR: {ctr:.2%}"
        )
        return report_text

    # ===== Задача 2: Получить метрики за последние 7 дней =====
    @task
    def get_last_7_days_metrics():
        """
        Собирает метрики за каждый из последних 7 дней.
        Возвращает DataFrame с колонками: date, dau, views, likes, ctr.
        """
        end = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        start = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

        query = f"""
        SELECT
            toDate(time) AS date,
            uniqExact(user_id) AS dau,
            sumIf(1, action = 'view') AS views,
            sumIf(1, action = 'like') AS likes,
            likes / views AS ctr
        FROM simulator_20260720.feed_actions
        WHERE toDate(time) BETWEEN '{start}' AND '{end}'
        GROUP BY date
        ORDER BY date
        """
        df = ph.read_clickhouse(query, connection=SOURCE_CONN)
        return df

    # ===== Задача 3: Построить график =====
    @task
    def create_plot(df_7days):
        """
        Строит графики (4 подграфика) для DAU, просмотров, лайков и CTR.
        Сохраняет изображение в байтовый поток BytesIO и возвращает его.
        """
        fig, axes = plt.subplots(2, 2, figsize=(12, 8))
        dates = pd.to_datetime(df_7days['date'])

        axes[0, 0].plot(dates, df_7days['dau'], marker='o', color='tab:blue')
        axes[0, 0].set_title('DAU')
        axes[0, 0].xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))

        axes[0, 1].plot(dates, df_7days['views'], marker='o', color='tab:orange')
        axes[0, 1].set_title('Просмотры')
        axes[0, 1].xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))

        axes[1, 0].plot(dates, df_7days['likes'], marker='o', color='tab:green')
        axes[1, 0].set_title('Лайки')
        axes[1, 0].xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))

        axes[1, 1].plot(dates, df_7days['ctr'], marker='o', color='tab:red')
        axes[1, 1].set_title('CTR')
        axes[1, 1].xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))

        for ax in axes.flat:
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
            ax.grid(True, linestyle='--', alpha=0.7)

        fig.suptitle('Метрики ленты новостей за последние 7 дней', fontsize=14)
        plt.tight_layout()

        # Сохраняем изображение в байтовый поток
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        plt.close(fig)
        return buf

    # ===== Закомментированная функция отправки в Telegram =====
    # @task
    # def send_telegram_report(report_text, plot_bytes):
    #     """
    #     Отправляет текстовое сообщение и фото с графиком в Telegram-чат.
    #     """
    #     bot = telegram.Bot(token=TOKEN)
    #     bot.send_photo(
    #         chat_id=CHAT_ID,
    #         photo=plot_bytes,
    #         caption=report_text
    #     )

    # ===== Новая задача: печать отчёта (замена Telegram) =====
    @task
    def print_report(report_text, plot_bytes):
        """
        Печатает текст отчёта в логи Airflow.
        График сохранён в plot_bytes, но не используется.
        """
        print("=" * 60)
        print(report_text)
        print("=" * 60)
        print("График сформирован и сохранён в переменную (байтовый поток).")
        # Закрываем поток, чтобы освободить память (опционально)
        plot_bytes.close()

    # ---------- Определение порядка выполнения задач ----------
    text = get_yesterday_metrics()
    df_7 = get_last_7_days_metrics()
    plot_buf = create_plot(df_7)

    # Вызываем print_report вместо закомментированного send_telegram_report
    print_report(text, plot_buf)

# Создаём экземпляр DAG
report_dag = report_dag()