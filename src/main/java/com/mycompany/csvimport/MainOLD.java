//package com.mycompany.csvimport;
//
//import com.univocity.parsers.csv.CsvParser;
//import com.univocity.parsers.csv.CsvParserSettings;
//import com.univocity.parsers.csv.CsvWriter;
//import com.univocity.parsers.csv.CsvWriterSettings;
//
//import java.io.*;
//import java.sql.Connection;
//import java.sql.ResultSet;
//import java.sql.Statement;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//import java.util.regex.Pattern;
//
//import com.google.gson.Gson;
//
//public class Main {
//    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.s Z");
//    private static final String createFunction = "CREATE OR REPLACE FUNCTION public.import_csv(csv_file text, target_table text)\n" +
//            " RETURNS character varying\n" +
//            " LANGUAGE plpgsql\n" +
//            "AS $function$\n" +
//            "DECLARE\n" +
//            "\tcolumn_ text;\n" +
//            "\tcmd VARCHAR ;\n" +
//            "\tarray_columns text[];\n" +
//            "\tdelimiter_ text;\n" +
//            "\ttable_size varchar;\n" +
//            "\t--StartTime timestamptz;\n" +
//            "  \t--EndTime timestamptz;\n" +
//            "\t--time_execution varchar;\n" +
//            "\t--count_ VARCHAR;\n" +
//            "begin\n" +
//            "\tCREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";\n" +
//            "\t--StartTime := clock_timestamp();\n" +
//            "\n" +
//            "\tcreate temp table import (line text) on commit drop;\n" +
//            "\n" +
//            "\t--Получим первую строку из файла (только Unix, Linux, Mac)\n" +
//            "\tcmd := 'head -n 1 ' || csv_file;\n" +
//            "\texecute format('COPY import from PROGRAM %L',cmd) ;\n" +
//            "\n" +
//            "\t--Тут мы определим какой разделитель используется в csv файле\n" +
//            "\t--Какой первый символ из указаных в [] попадется первым, он и будет разделителем\n" +
//            "\t--select substring(line, '[|'';\",=]') from import limit 1 into delimiter_;\n" +
//            "\tdelimiter_ = ',';\n" +
//            "\t--RAISE NOTICE 'Используемый разделитель %', delimiter_;\n" +
//            "\n" +
//            "\t--Полученую строку переведем в массив строк разделеными delimiter_\n" +
//            "\texecute format('select string_to_array(trim(line, ''()''), ''%s'') from import limit 1 ', delimiter_)into array_columns;\n" +
//            "\n" +
//            "\t--RAISE NOTICE 'Список столбцов: %', array_columns;\n" +
//            "\n" +
//            "\t--Создадим предварительную таблицу которая в конце будет переименована в указаное в параметрах желаемое навзвание\n" +
//            "\tDROP TABLE IF EXISTS temp_table;\n" +
//            "    create table temp_table ();\n" +
//            "\n" +
//            "\t--Создадим поля в таблице на основании массива\n" +
//            "\tFOREACH column_ IN ARRAY array_columns\n" +
//            "\tLOOP\n" +
//            "\t\texecute format('alter table temp_table add column %s text;', replace(column_,':','_'));\n" +
//            "\tEND LOOP;\n" +
//            "\n" +
//            "\t--Так как \" используется для обрамления значений по умолчанию то будет использоваться следующая конструкция\n" +
//            "\tif delimiter_ = '\"' then\n" +
//            "\t\texecute format('copy temp_table from %L with delimiter ''\"'' quote '''''' HEADER csv ', csv_file);\n" +
//            "\telse\n" +
//            "\t\texecute format('copy temp_table from %L with delimiter ''%s'' quote ''\"'' HEADER csv ', csv_file, delimiter_);\n" +
//            "\tend if;\n" +
//            "\n" +
//            "\texecute format('DROP TABLE IF EXISTS %I', target_table);\n" +
//            "\n" +
//            "\tif length(target_table) > 0 then\n" +
//            "\t\texecute format('alter table temp_table rename to %I', target_table);\n" +
//            "\tend if;\n" +
//            "\n" +
//            "\t--Добавим id\n" +
//            "\t--execute format('alter table %I add column \"id\" uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY;', target_table);\n" +
//            "\n" +
//            "\t--count_ := 0;\n" +
//            "\t--execute format('select count(*) from %I', target_table) into count_;\n" +
//            "\n" +
//            "\t--EndTime := clock_timestamp();\n" +
//            "    --time_execution := format ('Time = %s; ',EndTime - StartTime);\n" +
//            "\n" +
//            "\t--RETURN time_execution || 'Items = '  || count_;\n" +
//            "\tselect pg_relation_size(target_table) into table_size;\n" +
//            "\tRETURN table_size;\n" +
//            "end $function$\n" +
//            ";\n";
//
//
//    public static void main(String[] args) {
//        final Gson GSON = new Gson();
//        Util util = new Util();
//        try {
//            long startTime = System.currentTimeMillis();
//            if (args.length < 7) {
//                util.getLogs().add("Ошибка. Не все аргументы были указаны!");
//                throw new Exception("Ошибка. Не все аргументы были указаны!");
//            }
//            util.getLogs().add(DATE_FORMAT.format(new Date().getTime()) + " | Старт");
//
//            String inputFile = args[0];
//            String outputDir = args[1];
//            String outputDirInDocker = args[2];
//            String connectionString = args[3];
//            String user = args[4];
//            String pass = args[5];
//            String tableName = args[6];
//
////        String inputFile = "http://192.168.34.5:8090/Content/Jitsi/webgainsproducts.zip";
////        String outputDir = "d:/";
//
//            Connection connection = PostgresController.connectToDB(connectionString, user, pass);
//            if (connection == null) {
//                util.getLogs().add("Ошибка. Не удалось подключится к серверу Postgres!");
//                throw new Exception("Ошибка. Не удалось подключится к серверу Postgres!");
//            }
//
//            //Если файл из интернета
//            Pattern pattern = Pattern.compile("^(https?://)");
//            if (pattern.matcher(inputFile).find()) {
//                try {
//                    //Качаем файл
//                    inputFile = util.downloadFile(inputFile, outputDir);
//                } catch (IOException e) {
//                    util.getLogs().add("Ошибка. Не удалось скачать файл " + inputFile + " \n" + e.getMessage());
//                    throw new Exception("Ошибка. Не удалось скачать файл " + inputFile + " \n" + e.getMessage());
//                }
//            }
//            if (inputFile != null && !inputFile.isEmpty()) {
//                try {
//                    Pattern patternArchive = Pattern.compile("(\\.(gz|zip|7z|Z|bz2|lz|lz4|lzma|lzo|xz|zst|tar|ar|zx|cbz)$)"); //(\.csv)$
//                    int count = 0;
//                    while (patternArchive.matcher(inputFile).find()) {
//                        if (count == 2)
//                            break;//что бы не улететь в бесконечность, сделаем выход (обычно вложенность архива = 2)
//                        //Распаковка
//                        File tmpFile = new File(inputFile);
//                        inputFile = util.extract(tmpFile, outputDir);
//                        count++;
//                    }
//
//                } catch (IOException | InterruptedException e) {
//                    util.getLogs().add("Ошибка при попытке распаковать файл " + inputFile + " \n" + e.getMessage());
//                    throw new Exception("Ошибка при попытке распаковать файл " + inputFile + " \n" + e.getMessage());
//                }
//            } else {
//                util.getLogs().add("Ошибка. Не удалось скачать файл " + inputFile);
//                throw new Exception("Ошибка. Не удалось скачать файл " + inputFile);
//            }
//
//            File fileCSV = new File(inputFile);
//            if (!fileCSV.exists()) {
//                util.getLogs().add("Ошибка. Файл " + fileCSV.getAbsolutePath() + " не сущестует!");
//                throw new Exception("Ошибка. Файл " + fileCSV.getAbsolutePath() + " не сущестует!");
//            }
//
//            util.getLogs().add(DATE_FORMAT.format(new Date().getTime()) + " | Парсим файл " + inputFile);
//
//            //чтение
//            CsvParserSettings readSettings = new CsvParserSettings();
//            readSettings.getFormat().setLineSeparator("\n");
//            //readSettings.getFormat().setQuote('"');
//            //readSettings.getFormat().setDelimiter(',');
//            readSettings.setHeaderExtractionEnabled(true);
//            readSettings.setMaxCharsPerColumn(3000000);
//            //запись
//            CsvWriterSettings writerSettings = new CsvWriterSettings();
//            writerSettings.setQuoteAllFields(true);
//            //writerSettings.getFormat().setLineSeparator("\n");
//            //writerSettings.getFormat().setQuote('"');
//            //writerSettings.getFormat().setDelimiter(',');
//
//            int total = 0;
//            int error = 0;
//            int normal = 0;
//            String size = "0";
//
//            CsvParser parser = new CsvParser(readSettings);
//            parser.beginParsing(fileCSV);
//            String[] headers = parser.getRecordMetadata().headers();
//
//            int headersLength = headers.length;
//            File fileNewCSV = new File(outputDir + "/" + fileCSV.getName() + ".NEW.csv");
//            CsvWriter writer = new CsvWriter(fileNewCSV, writerSettings);
//            writer.writeHeaders(headers);
//
//            String[] row;
//            while ((row = parser.parseNext()) != null) {
//                total++;
//                if (row.length == headersLength) {
//                    normal++;
//                    writer.writeRow(row);
//                } else {
//                    error++;
//                }
//            }
//            parser.stopParsing();
//            writer.close();
//
//            fileCSV.delete();
//
//            util.getLogs().add(DATE_FORMAT.format(new Date().getTime()) + " | Импортируем в базу " + fileNewCSV.getName());
//            try (Statement stmt = connection.createStatement()) {
//                stmt.execute(createFunction);
//                String sql = "select * from public.import_csv('" + outputDirInDocker + "/" + fileNewCSV.getName() + "', '" + tableName + "');";
//                stmt.execute(sql);
//                ResultSet rs = stmt.executeQuery(sql);
//                while (rs.next()) {
//                    size = rs.getString(1);
//                }
//                rs.close();
//            }
//            connection.close();
//            fileNewCSV.delete();
//
//            long endTime = System.currentTimeMillis();
//            long duration = (endTime - startTime);
//            String time = String.format("%02d:%02d:%02d", duration / 1000 / 3600, duration / 1000 / 60 % 60, duration / 1000 % 60);
//
//            Response response = new Response();
//            response.setStatus("successful");
//            response.setMessage("Успешно");
//            response.setRuntime(time);
//            response.setTotal_items(total);
//            response.setSuccessful_items(normal);
//            response.setBad_items(error);
//            response.setTable_size(Long.valueOf(size));
//            System.out.println(GSON.toJson(response));
//
//        } catch (Exception e) {
//            Response response = new Response();
//            response.setStatus("error");
//            response.setMessage(e.getMessage());
//            String res = GSON.toJson(response);
//            System.out.println(res);
//        }
//    }
//}
//
////            StringBuilder stringBuilder = new StringBuilder();
////            InputStream is = Main.class.getResourceAsStream("/import_csv.sql");
////            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
////            String line;
////            while ((line = reader.readLine()) != null) {
////                stringBuilder.append(line);
////            }
