SELECT
    Start_month as Start_date,
    Courses as Courses,
    Grade as Grade,
    Start_Shift as Start_Shift,
    time_difference_since_start as time_difference_since_start,
    quantity_of_students as quantity_of_students,
    found_job as found_job,
    success_rate as success_rate
FROM(
        With found_job as (
            SELECT
                toStartOfMonth(Start_date) as Start_month,
                toStartOfMonth(now()) - Start_date as time_difference_since_start,
                Courses,
                Grade,
                Start_Shift,
                countIf(Full_name,time_difference between 1 and 90) as found_job
            FROM analytics.employment_rate
            WHERE Status in('Нашел работу','Нашел стажировку') and Student_hr != 'Влад'
        GROUP BY Start_date, Courses, Grade, Start_Shift
        HAVING time_difference_since_start between 28 and 62)

SELECT
    Start_month,
    Courses,
    Grade,
    Start_Shift,
    time_difference_since_start,
    quantity_of_students,
    found_job,
    round((found_job / quantity_of_students) * 100,2) as success_rate
from found_job
         right join(
    SELECT
        toStartOfMonth(Start_date) as Start_month,
        toStartOfMonth(now()) - Start_date as time_difference_since_start,
        Courses,
        Grade,
        Start_Shift,
        uniqExact(Full_name) as quantity_of_students
    FROM analytics.employment_rate
    WHERE Status not in('Пассивный поиск', 'Не ищет') and Student_hr != 'Влад'
    GROUP BY Start_date, Courses, Grade, Start_Shift
    HAVING time_difference_since_start between 28 and 62) as quantity_of_students
                   using(Start_month, Courses, Grade, Start_Shift, time_difference_since_start))