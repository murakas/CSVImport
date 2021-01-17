CREATE OR REPLACE FUNCTION "public"."import_csv"("csv_file" text, "target_table" text)
  RETURNS "pg_catalog"."varchar" AS $BODY$
DECLARE
	column_ text;
	cmd VARCHAR ;
	array_columns text[];
	delimiter_ text;
	--StartTime timestamptz;
  	--EndTime timestamptz;
	--time_execution varchar;
	--count_ VARCHAR;
begin
	CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
	--StartTime := clock_timestamp();

	create temp table import (line text) on commit drop;

	--Получим первую строку из файла (только Unix, Linux, Mac)
	cmd := 'head -n 1 ' || csv_file;
	execute format('COPY import from PROGRAM %L',cmd) ;

	--Тут мы определим какой разделитель используется в csv файле
	--Какой первый символ из указаных в [] попадется первым, он и будет разделителем
	--select substring(line, '[|'';",=]') from import limit 1 into delimiter_;
	delimiter_ = ',';
	--RAISE NOTICE 'Используемый разделитель %', delimiter_;

	--Полученую строку переведем в массив строк разделеными delimiter_
	execute format('select string_to_array(trim(line, ''()''), ''%s'') from import limit 1 ', delimiter_)into array_columns;

	--RAISE NOTICE 'Список столбцов: %', array_columns;

	--Создадим предварительную таблицу которая в конце будет переименована в указаное в параметрах желаемое навзвание
	DROP TABLE IF EXISTS temp_table;
    create table temp_table ();

	--Создадим поля в таблице на основании массива
	FOREACH column_ IN ARRAY array_columns
	LOOP
		execute format('alter table temp_table add column %s text;', replace(column_,':','_'));
	END LOOP;

    --Добавим id
    execute format('')

	--Так как " используется для обрамления значений по умолчанию то будет использоваться следующая конструкция
	if delimiter_ = '"' then
		execute format('copy temp_table from %L with delimiter ''"'' quote '''''' HEADER csv ', csv_file);
	else
		execute format('copy temp_table from %L with delimiter ''%s'' quote ''"'' HEADER csv ', csv_file, delimiter_);
	end if;

	execute format('DROP TABLE IF EXISTS %I', target_table);

	if length(target_table) > 0 then
		execute format('alter table temp_table rename to %I', target_table);
	end if;

	--Добавим id
	--execute format('alter table %I add column "id" uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY;', target_table);

	--count_ := 0;
	--execute format('select count(*) from %I', target_table) into count_;

	--EndTime := clock_timestamp();
    --time_execution := format ('Time = %s; ',EndTime - StartTime);

	--RETURN time_execution || 'Items = '  || count_;
	RETURN 'ok';
end $BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100