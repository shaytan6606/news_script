#!/usr/bin/python3

import sqlite3
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import smtplib
from datetime import datetime

conn = sqlite3.connect('news.sqlite', timeout=5.0, detect_types=0, isolation_level='DEFERRED', check_same_thread=True, factory=sqlite3.Connection, cached_statements=128, uri=False)
cursor = conn.cursor()

# Забираем HTML
URL_TEMPLATE = "https://www.cgko.ru/news/novosti-kadastrovoy-otsenki/"
r = requests.get(URL_TEMPLATE)

soup = bs(r.text, "html.parser")
news_board = soup.find_all('div', class_='col-md-12 col-sm-12')

# Очищаем стейджинг
cursor.execute( "delete from news_stg;" )

# Захватываем данные в стейджинг из супа
for x in news_board:
	cursor.execute(f"""
					insert into news_stg ( header, link, content, public_date, sent )
						values(
						'{x.h5.a.contents[0]}',
						replace('{'https://www.cgko.ru' + x.h5.a.get('href')}', '', ''),
						'{x.p.contents[0]}',
						'{x.find(class_='date').contents[2][1:11]}',
						'n');
            		""")

# Записываем НОВЫЕ данные в хранилище
cursor.execute("""
			insert into news ( header, link, content, public_date, sent )
			select
				stg.header, 
				stg.link, 
				stg.content, 
				stg.public_date,
				stg.sent
			from news_stg stg
			left join news dwh
			on stg.header = dwh.header and stg.public_date = dwh.public_date
			where dwh.header is null;
				""")

conn.commit()

# Получаем НОВЫЕ данные из хранилища в датафрейм
query = "select header, link, public_date from news where sent = 'n';"
df = pd.read_sql(query, conn)
arr = list(df.values)

# Создаем текст сообщения
massage = ""
for c in range(len(arr)):
	massage += f"<p>{arr[c][2]}</p>\n" + f"<a href=\"{arr[c][1]}\">{arr[c][0]}</a>\n\n\n"

def send_mail(to_adr, massage):
	MAIL_SERVER = 'smtp.yandex.ru'
	MAIL_PORT = 465

	MAIL_USERNAME = ''
	MAIL_PASSWORD = ''

	FROM = 'Mr. Robot <name@yandex.ru>'
	TO = to_adr

	smtpObj = smtplib.SMTP_SSL(MAIL_SERVER, MAIL_PORT)
	smtpObj.ehlo()
	smtpObj.login(MAIL_USERNAME, MAIL_PASSWORD)

	letter = "From: Mr. Robot <name@yandex.ru>\nTo: {}\nSubject: Новости кадастровой оценки\nContent-Type: text/html; charset=\"UTF-8\";\n\n{}".format(to_adr, massage)
	letter = letter.encode("UTF-8")

	smtpObj.sendmail(FROM, TO, letter)
	smtpObj.quit()

if massage == "":
	print('--------------------------------------------------')
	print('Сегодня новостей нет', datetime.now())
else:
	send_mail('name@yandex.ru', massage)
	send_mail('name@gmail.com', massage)
	print('--------------------------------------------------')
	print('Новости отправлены', datetime.now())

# Помечаем отправленные в сообщении данные как отправленные
cursor.execute("""
				update news
				set
					sent = 'y';
	""")

conn.commit()
cursor.close()
conn.close()

# print('https://www.cgko.ru/' + x.h5.a.get('href'))
# print(x.h5.a.contents[0])
# print(x.p.contents[0])
# print(x.find(class_='date').contents[2])

# ddl
# create table news_stg (
# 	header varchar(400), 
# 	link varchar(400), 
# 	content varchar(1000), 
# 	public_date char(10),
#  	sent char(1));


# create table news (
# 	header varchar(400), 
# 	link varchar(400), 
# 	content varchar(1000), 
# 	public_date char(10),
#  	sent char(1));
