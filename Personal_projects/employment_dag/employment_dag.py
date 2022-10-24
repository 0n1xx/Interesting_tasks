from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task
from clickhouse_driver import Client
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2022, 8, 11),
}
schedule_interval = "* 22 * * 7"


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def employment_dag():
    ch_client = Client(host=Variable.get("CLICKHOUSE_HOST"),
                       port=Variable.get("CLICKHOUSE_PORT"),
                       user=Variable.get("CLICKHOUSE_USER"),
                       password=Variable.get("CLICKHOUSE_PASSWORD"),
                       database='analytics')

    @task
    def extract_from_excel():
        sheet_id = "1WSxfMdjzHBaIa7AzKzjOL5gQCz595ytpUAPmbBvrGOg"
        df = pd.read_csv(f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv")
        df.rename(
            columns={"Дата старта": "Start_date", "Дата оффера": "Contract_date", "ФИО": "Full_name", "Пол": "Gender",
                     "Возраст": "Age", "Старт/смена": "Start_Shift", "Текущий статус": "Current_situation",
                     "Контакты": "Contacts", "Почта": "Mail", "Город": "City_Country", "Курс": "Courses",
                     "Профессия": "Grade", "В поиске": "Status", "Пришел из профессии": "Previous_experience",
                     "Последнее место работы ": "Last_job", "Трудоустроен в": "Current_Job",
                     "номер потока": "Class_number", "дата старта потока": "Start_Class_date",
                     "логин студента": "student_login", "Кто ведет": "Student_hr", "Unnamed: 0": "Student_id"},
            inplace=True)
        df.drop(["Ссылка на папку студента", "Ссылка на резюме", "Дополнительно",
                 "Комментарий после карьерной консультации", "Current_situation", "Previous_experience"], axis=1,
                inplace=True)
        df[["Start_date", "Contract_date", "Start_Class_date"]] = df[
            ["Start_date", "Contract_date", "Start_Class_date"]].apply(lambda x: pd.to_datetime(x, dayfirst=True))
        return df

    @task
    def jobs_score_rate(df):
        df["time_difference"] = (df["Contract_date"] - df["Start_date"]).dt.days
        lst_job_scores = []
        for i in df.time_difference:
            if i != "nan" and i != 0 and 0 < i <= 90:
                lst_job_scores.append("Success")
            elif i != "nan" and i != 0 and i > 90:
                lst_job_scores.append("Fail")
            else:
                lst_job_scores.append("Unknown")
        df["success_rate"] = lst_job_scores
        df.time_difference.fillna(0,
                                  inplace=True)
        df_final = df.query("time_difference >= 0")  # Проверка чтобы все точно нормально отработало
        return df_final

    @task
    def calculate_age(df):
        lst = []
        for i in df.Age:
            if 16 <= i <= 25:
                lst.append("first_group")
            elif 25 < i <= 35:
                lst.append("second_group")
            elif i > 35:
                lst.append("third_group")
            else:
                lst.append("Unknown")
        df["age_group"] = lst

        df["Age"].fillna(0, inplace=True)
        df["Age"] = df["Age"].astype("int32")
        return df

    @task
    def filter_df(df):

        df[
            ["Full_name", "Gender", "Contacts", "Mail", "City_Country", "Courses", "Grade", "Status", "Current_Job",
             "student_login", "Student_hr", "Last_job", "success_rate", "age_group"]] = df[
            ["Full_name", "Gender", "Contacts", "Mail", "City_Country", "Courses", "Grade", "Status", "Current_Job",
             "student_login", "Student_hr", "Last_job", "success_rate", "age_group"]].astype(str)
        df.student_login = df.student_login.replace("nan", "Unknown")
        df.Student_hr = df.Student_hr.replace("nan", "Unknown")

        df["Contract_date"].fillna("2000-01-01", inplace=True)
        df["Start_date"].fillna("2000-01-01", inplace=True)
        df["Start_Class_date"].fillna("2000-01-01", inplace=True)
        df.replace("nan", "Unknown", inplace=True)

        df.Start_Shift.fillna(0.0, inplace=True)
        df.Class_number.fillna(-5.0, inplace=True)

        df.Status = df.Status.replace("Устроен на работу", "Нашел работу")
        df.City_Country = df.City_Country.replace("Saint Petersburg", "Saint-Petersburg")

        return df

    @task
    def clean_upload_to_clickhouse(df):
        ch_client.execute("TRUNCATE analytics.employment_rate")
        ch_client.execute("INSERT INTO analytics.employment_rate VALUES", df.to_dict(orient="records"))
        ch_client.execute('OPTIMIZE TABLE analytics.employment_rate DEDUPLICATE BY Student_id')

    df_extract_from_excel = extract_from_excel()
    df_jobs_score = jobs_score_rate(df_extract_from_excel)
    df_age_group = calculate_age(df_jobs_score)
    df_filter_data = filter_df(df_age_group)
    clean_upload_to_clickhouse(df_filter_data)


employment_dag = employment_dag()
