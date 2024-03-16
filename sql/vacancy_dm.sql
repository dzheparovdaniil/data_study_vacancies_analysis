CREATE OR REPLACE VIEW vacancies.vacancies_dm AS 
SELECT *
FROM (
SELECT 
id,
name,
area_name,
experience_name,
REPLACE(substring(professional_roles, 2, length(professional_roles)-2), '''', '"')::jsonb ->> 'id' AS professional_role_id,
REPLACE(substring(professional_roles, 2, length(professional_roles)-2), '''', '"')::jsonb ->> 'name' AS professional_role_name,
employer_name,
published_at,
CASE WHEN address_city = 'Минск' THEN salary_from * 28 ELSE salary_from END AS salary_from,
CASE WHEN address_city = 'Минск' THEN salary_to * 28 ELSE salary_to END AS salary_to,
round((COALESCE(salary_to, salary_from, 0) + COALESCE(salary_from, salary_to, 0))/2) AS salary_avg,
employment_name,
address_raw,
address_city,
address_lat,
address_lng,
alternate_url,
apply_alternate_url,
archived,
CASE 
	WHEN upper(skill) LIKE('%POWER%') THEN 'POWER STACK (BI, QUERY, PIVOT)'
	WHEN upper(skill) LIKE('%WAREHOUSE%') 
		OR upper(skill) LIKE('%DWH%') 
		OR upper(skill) LIKE('%ХРАНИЛИЩЕ%') 
		OR upper(skill) LIKE('%КХД%')
		OR upper(skill) LIKE('%LAKE%')
	THEN 'DWH/DATA LAKE'
	WHEN upper(skill) LIKE('%SUPERSET%') THEN 'APACHE SUPERSET'
	WHEN upper(skill) LIKE('%AIRFLOW%') THEN 'APACHE AIRFLOW'
	WHEN upper(skill) LIKE('%SPARK%') THEN 'APACHE SPARK'
	WHEN upper(skill) LIKE('%АНАЛИЗ%') 
		OR upper(skill) LIKE('%АНАЛИТИКА%') 
		OR upper(skill) LIKE('%АНАЛИТИЧЕСКИЕ%') 
		OR upper(skill) LIKE('%АНАЛИТИЧЕСКИЙ%') 
		OR upper(skill) LIKE('%ANALYSIS%') 
		OR upper(skill) LIKE('%ANALYTICAL%') 
		OR upper(skill) LIKE('%ANALYTICS%') 
		OR upper(skill) LIKE('%АНАЛИТИЧЕСКОЕ МЫШЛЕНИЕ%')
	THEN 'АНАЛИТИЧЕСКИЕ ИССЛЕДОВАНИЯ'
	WHEN upper(skill) LIKE('BI %') 
		OR upper(skill) LIKE('% BI') 
		OR upper(skill) LIKE('%ПОДГОТОВКА ОТЧЕТОВ%') 
		OR upper(skill) LIKE('%BI-РАЗРАБОТЧИК%') 
		OR upper(skill) LIKE('%BI-ОТЧЁТЫ%') 
		OR upper(skill) LIKE('%ВИЗУАЛИЗАЦИЯ%')
		OR upper(skill) LIKE('%B%SINESS INTELLIGENCE%')
		OR upper(skill) LIKE('%ДАШБОРД%')
	THEN 'BI'
	WHEN upper(skill) LIKE('%EXCEL%') THEN 'EXCEL'
	WHEN upper(skill) LIKE('%ETL%') THEN 'ETL'
	WHEN upper(skill) LIKE('%LENS%') THEN 'YANDEX DATALENS'
	WHEN upper(skill) LIKE('%CONFLUENCE%') THEN 'ATLASSIAN CONFLUENCE'
	WHEN upper(skill) LIKE('%JIRA%') OR upper(skill) LIKE('%ДЖИРА%') THEN 'ATLASSIAN JIRA'
	WHEN upper(skill) LIKE('%СУБД%') 
		OR upper(skill) LIKE('%БАЗЫ ДАННЫХ%') 
		OR upper(skill) LIKE('%БАЗА ДАННЫХ%') 
		OR upper(skill) LIKE('%РАБОТА С БАЗАМИ ДАННЫХ%') 
	THEN 'БАЗЫ ДАННЫХ'
	WHEN upper(skill) LIKE('%1C%') OR upper(skill) LIKE('%1С%') THEN '1C'
	WHEN UPPER(skill) LIKE('%АНГЛИЙСКИЙ%') THEN 'АНГЛИЙСКИЙ ЯЗЫК'
	WHEN UPPER(skill) LIKE('%A/B%') 
		OR UPPER(skill) LIKE('%ABC ANALYSIS%') 
		OR UPPER(skill) LIKE('%AB %') 
		OR UPPER(skill) LIKE('%А/Б%')
		OR UPPER(skill) LIKE('%А/В%')
	THEN 'A/B ТЕСТЫ'
	WHEN UPPER(skill) LIKE('%AD%HOC%') THEN 'ADHOC ЗАДАЧИ'
	WHEN UPPER(skill) LIKE('%API%') OR UPPER(skill) LIKE('%АПИ%') OR UPPER(skill) LIKE('%REST%') THEN 'API'
	WHEN UPPER(skill) LIKE('%B2B%') OR UPPER(skill) LIKE('%B2C%') THEN 'B2B/B2C'
	WHEN UPPER(skill) LIKE('%BITRIX%') OR UPPER(skill) LIKE('%БИТРИКС%') OR UPPER(skill) LIKE('БИТ%ФИНАНС%') THEN 'BITRIX'
	WHEN UPPER(skill) LIKE('%BPM%') OR UPPER(skill) LIKE('%BPNM%') THEN 'BPMN'
	WHEN UPPER(skill) LIKE('%PYTHON%') THEN 'PYTHON'
	WHEN UPPER(skill) LIKE('%QLIK%SENS%') THEN 'QLIKSENS'
	WHEN UPPER(skill) LIKE('%QLIK%VIEW%') THEN 'QLIKVIEW'
	WHEN UPPER(skill) LIKE('%KAFKA%') OR UPPER(skill) LIKE('%БРОКЕР%СООБЩЕНИЙ%') OR UPPER(skill) LIKE('%RABBIT%') THEN 'БРОКЕРЫ СООБЩЕНИЙ (KAFKA, RABBIT)'
	WHEN UPPER(skill) LIKE('%SQL%') THEN 'SQL'
	WHEN UPPER(skill) LIKE('%XML%') OR UPPER(skill) LIKE('%JSON%') THEN 'XML/JSON'
	WHEN UPPER(skill) LIKE('%АВТОМАТИЗАЦИЯ%') THEN 'АВТОМАТИЗАЦИЯ ПРОЦЕССОВ'
	WHEN UPPER(skill) LIKE('%УПРАВЛЕНИЕ%') THEN 'УПРАВЛЕНЧЕСКИЕ НАВЫКИ'
	WHEN UPPER(skill) = '' OR skill IS NULL THEN 'EMPTY'
	WHEN upper(skill) LIKE('%AS IS%') THEN 'AS IS/TO BE MAPPING'
	WHEN upper(skill) LIKE('MS%') OR upper(skill) LIKE('%MICROSOFT%') THEN 'MS STACK (OFFICE, STUDIO, OUTLOOK)'
	WHEN upper(skill) LIKE('%OLAP%') OR upper(skill) LIKE('%OLTP%') THEN 'OLAP/OLTP'
	WHEN upper(skill) LIKE('%ORACLE%') THEN 'ORACLE STACK (DB, CRM)'
	WHEN upper(skill) LIKE('SAP%') THEN 'SAP STACK'
	WHEN upper(skill) LIKE('SAS%') THEN 'SAP STACK'
	WHEN upper(skill) LIKE('%USER%STORIES%') OR upper(skill) LIKE('%USER%STORY%') THEN 'OLAP/OLTP'
	WHEN upper(skill) LIKE('%ГОСТ%') THEN 'ГОСТ'
	WHEN upper(skill) LIKE('%ГРАМОТНАЯ%РЕЧЬ%') THEN 'ГРАМОТНАЯ РЕЧЬ'
	WHEN upper(skill) LIKE('%ОБУЧЕНИЕ%') OR upper(skill) LIKE('%ОБУЧАЕМОСТЬ%') THEN 'ОБУЧЕНИЕ ДРУГИХ/ОБУЧАЕМОСТЬ'
	WHEN upper(skill) LIKE('%ОТВЕТСТВЕННОСТЬ%') THEN 'ОТВЕТСТВЕННОСТЬ'
	WHEN upper(skill) LIKE('%МАТЕМАТ%') THEN 'МАТЕМАТИЧЕСКИЕ НАВЫКИ'
	ELSE upper(skill)
END AS skill_new
FROM vacancies.vacancy v
LEFT JOIN vacancies.skill s
ON v.id = s.vacancy_id
) AS t
WHERE professional_role_id::integer IN 
(
165, 156, 164
)
AND area_name NOT IN ('Абакан', 'Алматы', 'Астана', 'Батуми', 'Бишкек', 'Кипр', 'Сербия', 'США', 'Ташкент', 'Тбилиси')
