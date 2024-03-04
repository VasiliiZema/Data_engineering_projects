**Практическое задание №4**

Студент: РИМ-130908 - Земов В.А.


Данный репозиторий написан для выполнения учебно-практической работы по дисциплине "Инженерия данных".

**Задача:**


**task1_2/exercise_ID4_1:**

Дан файл с некоторыми данными. Формат файла – произвольный. Спроектируйте на его основе и создайте таблицу в базе данных (SQLite). Считайте данные из файла и запишите их в созданную таблицу. Реализуйте и выполните следующие запросы:

	вывод первых (VAR+10) отсортированных по произвольному числовому полю строк из таблицы в файл формата json;

	вывод (сумму, мин, макс, среднее) по произвольному числовому полю;

	вывод частоты встречаемости для категориального поля;

	вывод первых (VAR+10) отфильтрованных по произвольному предикату отсортированных по произвольному числовому полю строк из таблицы в файл формате json.



**task1_2/exercise_ID3_2:**

Дан файл с некоторыми данными. Формат файла – произвольный. Спроектируйте на его основе и создайте таблицу в базе данных (SQLite). Считайте данные из файла и запишите их в созданную таблицу. Реализуйте и выполните следующие запросы:
	вывод первых (VAR+10) отсортированных по произвольному числовому полю строк из таблицы в файл формата json;
	вывод (сумму, мин, макс, среднее) по произвольному числовому полю;
	вывод частоты встречаемости для категориального поля;
	вывод первых (VAR+10) отфильтрованных по произвольному предикату отсортированных по произвольному числовому полю строк из таблицы в файл формате json.



**task3/exercise_ID4_3:**

Дано два файла разных форматов. Необходимо проанализировать их структуру и выделить общие хранимые данные. Необходимо создать таблицу для хранения данных в базе данных. Произведите запись данных из файлов разных форматов в одну таблицу. Реализуйте и выполните следующие запросы:

	вывод первых (VAR+10) отсортированных по произвольному числовому полю строк из таблицы в файл формата json;

	вывод (сумму, мин, макс, среднее) по произвольному числовому полю;

	вывод частоты встречаемости для категориального поля;

	вывод первых (VAR+15) отфильтрованных по произвольному предикату отсортированных по произвольному числовому полю строк из таблицы в файл формате json.



**task4/exercise_ID4_4:**

Дан набор файлов. В одних содержится информация о некоторых товарах, которые нужно сохранить в соответствующей таблице базы данных. В других (начинающихся с префикса upd) содержится информация об изменениях, которые могут задаваться разными командами: изменение цены, изменение остатков, снять/возврат продажи, удаление из каталога (таблицы). По одному товару могут быть несколько изменений, поэтому при создании таблицы необходимо предусмотреть поле-счетчик, которое инкрементируется каждый раз, когда происходит обновление строки. Все изменения необходимо производить, используя транзакции, проверяя изменения на корректность (например, цена или остатки после обновления не могут быть отрицательными)
После записи всех данные и применения обновлений необходимо выполнить следующие запросы:

	вывести топ-10 самых обновляемых товаров

	проанализировать цены товаров, найдя (сумму, мин, макс, среднее) для каждой группы, а также количество товаров в группе

	проанализировать остатки товаров, найдя (сумму, мин, макс, среднее) для каждой группы товаров

	произвольный запрос 



**task5/exercise_ID4_5(В разработке, планирую сдать позже):**

Подобрать пару наборов данных разных форматов. Создать базу данных минимум на три таблицы. Заполнение данных осуществляем из файлов. Реализовать выполнение 6-7 запросов к базе данных с выводом результатов в json. Среди них могут быть:

	выборка с простым условием + сортировка + ограничение количество

	подсчет объектов по условию, а также другие функции агрегации

	группировка

	обновление данных

В решении необходимо указать:

	название и описание предметной области (осмысленное)

	SQL для создания таблиц

	файлы исходных данных (можно обрезать до такого размера, чтобы влезли в GitHub)

	скрипт для инициализации базы данных (создание таблиц)

	скрипт для загрузки данных из файлов в базу данных

	файл базы данных

	скрипт с выполнением запросов к базе данных 
