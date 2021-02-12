docker run -ti -d --name pg_docker -e POSTGRES_PASSWORD=168 -p 2345:5432 -v /home/murad/postgresData:/var/lib/postgresql/data -v /home/murad:/home/murad postgres


ЗАПУСК В РЕЖИМЕ КОМАНДНОЙ СТРОКИ:
	java -cp CSVimport_1.0.jar com.mycompany.csvimport.Console "http://192.168.34.5:8090/Content/Jitsi/webgainsproducts.tar" /home/murad/download /home/murad/download 192.168.35.89:2345/test postgres 168 table1

	Аргументы:
		"http://192.168.34.5:8090/Content/Jitsi/webgainsproducts.tar" 	- URL/Путь откуда качаем файл (нужно указывать в ковычках)
		/home/murad/download 										  	- папка куда будет скачен файл
		/home/murad/download											- папка в docker где буде лежать csv
		192.168.35.89:2345/test											- ip сервера postgres
		postgres 														- пользователь
		168																- пароль
		murad															- название таблицы в базе

ЗАПУСК В РЕЖИМЕ СЕРВИСА НА ПОРТУ 8844:
	java -cp CSVimport_0.6.jar com.mycompany.csvimport.Service 

	POST запрос {ip}:8844/rest/import с параметрами:
		String inputFile			- URL/Путь откуда качаем файл (НЕ нужно указывать в ковычках)
		String outputDir 			- папка куда будет скачен файл
		String outputDirInDocker	- папка в docker где буде лежать csv
		String connectionString		- ip сервера postgres
		String user					- пользователь
		String pass 				- пароль
		String tableName			- название таблицы в базе
