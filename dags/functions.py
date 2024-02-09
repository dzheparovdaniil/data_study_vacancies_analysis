from datetime import datetime 
import pandas as pd
import requests as r
import json
import time
from pandas.io.json import json_normalize
import ast
import sqlalchemy as sa
import telebot
from loguru import logger
from airflow.models import Variable

host = Variable.get("host")
login = Variable.get("login")
password = Variable.get("password")
access_token = Variable.get("access_token")
bot_token = Variable.get("bot_token")
chat_id = Variable.get("chat_id")


def postgres_con():
      """функция подключения к БД"""
      engine =sa.create_engine(f'postgresql://{login}:{password}@{host}/postgres') 
      con_postgres = engine.connect()
      return con_postgres

def get_vacancies_all():
      """функция получения списка url вакансий из API запроса"""
      con = postgres_con()
      req = r.get('https://api.hh.ru/vacancies?area=1&ored_clusters=true&professional_role=156&professional_role=165&search_period=1') 
      data = req.content.decode()
      req.close()
      raw_data = json.loads(data)
      vacancies_count = raw_data['found']
      pages = raw_data['pages']
      logger.info(f'found {vacancies_count} vacancies in {pages} pages') 
      for page in range(0,pages,1):
            params = {
            'page': page,
            'per_page': 20 
            }
            req_v = r.get('https://api.hh.ru/vacancies?area=1&ored_clusters=true&professional_role=156&professional_role=165&search_period=1', params=params) 
            data_v = req_v.content.decode()
            req_v.close()
            raw_data_v = json.loads(data_v)
            df_v = pd.json_normalize(raw_data_v['items'])
            df_v[['url', 'id']].to_sql(con=con, schema='vacancies', name='url', if_exists='append', index=False)
            logger.info(f'loaded {df_v.shape[0]} urls')

def get_urls(con):
      """функция получения списка url из БД для парсинга каждой вакансии, которая еще не записана в БД"""
      sql_query = """
      SELECT DISTINCT url
      FROM vacancies.url
      WHERE id NOT IN (SELECT DISTINCT id::integer FROM vacancies.vacancy)
      """
      df = pd.read_sql(sql = sql_query, con=con)
      urls_list = list(df['url'])
      return urls_list


def parse_data(row):
      """функция для парсинга значения навыка из представленного словаря"""
      dict_data = ast.literal_eval(str(row))  
      return dict_data['name']  


def main(data):
      """функция для трансформации данных и подготовка к записи в БД"""
      if 'salary.from' not in data.columns:
           data['salary.from'] = 0

      if 'salary.to' not in data.columns:
            data['salary.to'] = 0

      if 'salary.gross' not in data.columns:
            data['salary.gross'] = False

      if 'address.raw' not in data.columns:
            data['address.raw'] = ''
      
      if 'address.city' not in data.columns:
            data['address.city'] = ''
      
      if 'address.lat' not in data.columns:
            data['address.lat'] = 0
      
      if 'address.lng' not in data.columns:
            data['address.lng'] = 0
      
      df_vacancy = data[['id', 'name', 'area.name', 'experience.name', 'employer.name', 
       'response_url', 'specializations', 'professional_roles', 'published_at', 'salary.from', 'salary.to', 
       'salary.gross', 'employment.name', 'address.raw', 'address.city', 'address.lat', 'address.lng', 'alternate_url', 'apply_alternate_url', 'archived']]
      df_vacancy['working_days'] = ''
      df_skill = data[['id', 'key_skills']]
      df_skill = df_skill.explode('key_skills')
      if df_skill['key_skills'].isna().values[0] == True:
            df_skill['skill'] = ''
      else: 
            df_skill['skill'] = df_skill['key_skills'].apply(lambda row: parse_data(row))
      df_vacancy.columns = df_vacancy.columns.str.replace('.', '_')
      df_vacancy = df_vacancy[['id', 'name', 'area_name', 'experience_name', 'employer_name',
        'response_url', 'specializations', 'professional_roles', 'published_at', 'working_days', 'salary_from', 'salary_to',
        'salary_gross', 'employment_name', 'address_raw', 'address_city', 'address_lat', 'address_lng', 'alternate_url', 'apply_alternate_url', 'archived']]
      df_vacancy['load_date'] = datetime.now().strftime('%Y-%m-%d')
      df_vacancy.professional_roles = df_vacancy.professional_roles.astype('string').str.replace('[','{')
      df_vacancy.professional_roles = df_vacancy.professional_roles.astype('string').str.replace(']','}')

      df_skill.columns = df_skill.columns.str.replace('.', '_')
      df_skill.rename(columns={'id': 'vacancy_id'}, inplace=True)
      df_skill = df_skill[['vacancy_id', 'skill']]
      return df_vacancy, df_skill

def parse_vacancy(url, con):
    """функция парсинга вакансии по полученному url, трансформации данных и запись в БД"""
    delete_sql = f"""
    DELETE FROM vacancies.url
    WHERE url = '{url}'
    """
    params = {"access_token": access_token, 
              "token_type": "bearer"}
    try:
        response = r.get(url, params=params)
        logger.info(f'got data for {url}')
        raw_data = json.loads(response.text)
        if raw_data['description'] == 'Not Found':
            logger.info(f'vacancy not found, deleting {url} from list')
            con.execute(delete_sql)
        else:
            df = pd.json_normalize(raw_data)
            df_vacancy, df_skill = main(df)
            if df_skill.shape[0]==0:
                  logger.info(f'vacancy {url} do not have skills, loading only vacancy')
                  df_vacancy.to_sql(con=con, schema='vacancies', name='vacancy', if_exists='append', index=False)
                  time.sleep(10)
            else:
                  df_vacancy.to_sql(con=con, schema='vacancies', name='vacancy', if_exists='append', index=False)
                  df_skill.to_sql(con=con, schema='vacancies', name='skill', if_exists='append', index=False)
                  logger.info(f'load vacancy and skill data for {url}')
                  time.sleep(10)
    except Exception as e:
         logger.info(f'cannot get some attributes for {url}')
         raise e 

     
def send_notification(text):
      """функция отправки уведомлений через телеграм-бота"""
      bot = telebot.TeleBot(bot_token)
      bot.send_message(chat_id = chat_id, text=text)

     
def parse_vacancies():
      """функция парсинга вакансий в цикле по каждой url из списка. 
         ограничиваем на 100 вакансий чтобы не словить капчу"""
      con = postgres_con()
      urls = get_urls(con)
      try:
            for url in urls[:100]:
                  parse_vacancy(url, con)
            con.close()
            send_notification('SUCCESS: successfully load vacancies')
      except Exception as e:
            send_notification(f'FAIL: unable to load vacancies / {e}')
            raise e