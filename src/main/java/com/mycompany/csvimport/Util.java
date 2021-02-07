package com.mycompany.csvimport;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.*;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;

public class Util {

    private final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Connection con;
    private final boolean isTerminal;
    private final String createFunction = "CREATE OR REPLACE FUNCTION public.import_csv(csv_file text, target_table text)\n" +
            " RETURNS character varying\n" +
            " LANGUAGE plpgsql\n" +
            "AS $function$\n" +
            "DECLARE\n" +
            "\tcolumn_ text;\n" +
            "\tcmd VARCHAR ;\n" +
            "\tarray_columns text[];\n" +
            "\tdelimiter_ text;\n" +
            "\ttable_size varchar;\n" +
            "\t--StartTime timestamptz;\n" +
            "  \t--EndTime timestamptz;\n" +
            "\t--time_execution varchar;\n" +
            "\t--count_ VARCHAR;\n" +
            "begin\n" +
            "\tCREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";\n" +
            "\t--StartTime := clock_timestamp();\n" +
            "\n" +
            "\tcreate temp table import (line text) on commit drop;\n" +
            "\n" +
            "\t--Получим первую строку из файла (только Unix, Linux, Mac)\n" +
            "\tcmd := 'head -n 1 ' || csv_file;\n" +
            "\texecute format('COPY import from PROGRAM %L',cmd) ;\n" +
            "\n" +
            "\t--Тут мы определим какой разделитель используется в csv файле\n" +
            "\t--Какой первый символ из указаных в [] попадется первым, он и будет разделителем\n" +
            "\t--select substring(line, '[|'';\",=]') from import limit 1 into delimiter_;\n" +
            "\tdelimiter_ = ',';\n" +
            "\t--RAISE NOTICE 'Используемый разделитель %', delimiter_;\n" +
            "\n" +
            "\t--Полученую строку переведем в массив строк разделеными delimiter_\n" +
            "\texecute format('select string_to_array(trim(line, ''()''), ''%s'') from import limit 1 ', delimiter_)into array_columns;\n" +
            "\n" +
            "\t--RAISE NOTICE 'Список столбцов: %', array_columns;\n" +
            "\n" +
            "\t--Создадим предварительную таблицу которая в конце будет переименована в указаное в параметрах желаемое навзвание\n" +
            "\tDROP TABLE IF EXISTS temp_table;\n" +
            "    create table temp_table ();\n" +
            "\n" +
            "\t--Создадим поля в таблице на основании массива\n" +
            "\tFOREACH column_ IN ARRAY array_columns\n" +
            "\tLOOP\n" +
            "\t\texecute format('alter table temp_table add column %s text;', replace(column_,':','_'));\n" +
            "\tEND LOOP;\n" +
            "\n" +
            "\t--Так как \" используется для обрамления значений по умолчанию то будет использоваться следующая конструкция\n" +
            "\tif delimiter_ = '\"' then\n" +
            "\t\texecute format('copy temp_table from %L with delimiter ''\"'' quote '''''' HEADER csv ', csv_file);\n" +
            "\telse\n" +
            "\t\texecute format('copy temp_table from %L with delimiter ''%s'' quote ''\"'' HEADER csv ', csv_file, delimiter_);\n" +
            "\tend if;\n" +
            "\n" +
            "\texecute format('DROP TABLE IF EXISTS %I', target_table);\n" +
            "\n" +
            "\tif length(target_table) > 0 then\n" +
            "\t\texecute format('alter table temp_table rename to %I', target_table);\n" +
            "\tend if;\n" +
            "\n" +
            "\t--Добавим id\n" +
            "\t--execute format('alter table %I add column \"id\" uuid NOT NULL DEFAULT uuid_generate_v4() PRIMARY KEY;', target_table);\n" +
            "\n" +
            "\t--count_ := 0;\n" +
            "\t--execute format('select count(*) from %I', target_table) into count_;\n" +
            "\n" +
            "\t--EndTime := clock_timestamp();\n" +
            "    --time_execution := format ('Time = %s; ',EndTime - StartTime);\n" +
            "\n" +
            "\t--RETURN time_execution || 'Items = '  || count_;\n" +
            "\tselect pg_relation_size(target_table) into table_size;\n" +
            "\tRETURN table_size;\n" +
            "end $function$\n" +
            ";\n";

    public Util(boolean isTerminal) {
        this.isTerminal = isTerminal;
    }

    //    public File getFile(String fileURL, String saveDir) {
//        File outputFile = null;
//        HttpURLConnection httpConn;
//        try {
//            URL url = new URL(fileURL);
//            httpConn = (HttpURLConnection) url.openConnection();
//            int responseCode = httpConn.getResponseCode();
//            if (responseCode == HttpURLConnection.HTTP_OK) {
//                String fileName = null;
//                String disposition = httpConn.getHeaderField("Content-Disposition");
//                if (disposition != null) {
//                    int index = disposition.indexOf("filename=") - 1;
//                    if (index > 0) {
//                        fileName = disposition.substring(index + 10);
//                    }
//                } else {
//                    fileName = fileURL.substring(fileURL.lastIndexOf("/") + 1);
//                }
//
//                System.out.println("LOG: " + DATE_FORMAT.format(new Date().getTime()) + " | Download file " + fileName);
//
//                if (fileName != null) {
//                    fileName = fileName.replaceAll("([,\\\\:*?\"<>|+%!@#$^&=;])", "");
//                    if (fileName.length() > 250) fileName = fileName.substring(250);
//                }
//
//                InputStream inputStream = httpConn.getInputStream();
//                String saveFilePath = saveDir + File.separator + fileName;
//
//                ReadableByteChannel channel = Channels.newChannel(inputStream);
//                outputFile = new File(saveFilePath);
//                FileOutputStream outputStream = new FileOutputStream(outputFile);
//                outputStream.getChannel().transferFrom(channel, 0, Long.MAX_VALUE);
//                outputStream.close();
//                channel.close();
//                inputStream.close();
//                httpConn.disconnect();
//
//            } else {
//                if (!isTerminal)
//                    System.out.println("LOG: " + DATE_FORMAT.format(new Date().getTime()) + " | An error occurred while trying to download the file. Server returned HTTP code: " + responseCode);
//            }
//            httpConn.disconnect();
//        } catch (IOException e) {
//            if (!isTerminal)
//                System.out.println("LOG: " + DATE_FORMAT.format(new Date().getTime()) + " | Unknown error while trying to download a file: " + e.getMessage());
//        }
//        return outputFile;
//    }
    public File getFile(String fileURL, String saveDir) {
        HttpURLConnection httpConn;
//        String disposition = httpConn.getHeaderField("Content-Disposition");
//        File outputFile = downloadFile(fileURL, saveDir, disposition, httpConn.getInputStream());


        try {
            URL url = new URL(fileURL);
            httpConn = (HttpURLConnection) url.openConnection();
            short status = -1;
            int responseCode = httpConn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                status = 0;
            }
            if (responseCode == HttpURLConnection.HTTP_MOVED_TEMP ||
                    responseCode == HttpURLConnection.HTTP_MOVED_PERM ||
                    responseCode == HttpURLConnection.HTTP_SEE_OTHER) {
                status = 1;
            }

            if(status = 1)

            httpConn.disconnect();
        } catch (IOException e) {
            if (!isTerminal)
                System.out.println("LOG: " + DATE_FORMAT.format(new Date().getTime()) + " | Unknown error while trying to download a file: " + e.getMessage());
        }


        return outputFile;
    }

    private File downloadFile(String fileURL, String saveDir, String disposition, InputStream inputStream) throws IOException {
        String fileName = null;
        if (disposition != null) {
            int index = disposition.indexOf("filename=") - 1;
            if (index > 0) {
                fileName = disposition.substring(index + 10);
            }
        } else {
            fileName = fileURL.substring(fileURL.lastIndexOf("/") + 1);
        }

        System.out.println("LOG: " + DATE_FORMAT.format(new Date().getTime()) + " | Download file " + fileName);

        if (fileName != null) {
            fileName = fileName.replaceAll("([,\\\\:*?\"<>|+%!@#$^&=;])", "");
            if (fileName.length() > 250) fileName = fileName.substring(250);
        }

        String saveFilePath = saveDir + File.separator + fileName;

        ReadableByteChannel channel = Channels.newChannel(inputStream);
        File outputFile = new File(saveFilePath);
        FileOutputStream outputStream = new FileOutputStream(outputFile);
        outputStream.getChannel().transferFrom(channel, 0, Long.MAX_VALUE);
        outputStream.close();
        channel.close();
        inputStream.close();

        return outputFile;
    }

    public File extract(String filePath, String outputDir) {
        String path = filePath;
        Pattern patternArchive = Pattern.compile("(\\.(gz|zip|7z|Z|bz2|lz|lz4|lzma|lzo|xz|zst|tar|ar|zx|cbz)$)"); //(\.csv)$
        int count = 0;
        while (patternArchive.matcher(path).find()) {
            if (count == 2)
                break;//что бы не улететь в бесконечность, сделаем выход (обычно вложенность архива = 2)
            //Распаковка
            File tmpFile = new File(path);
            path = extractCore(tmpFile, outputDir);
            tmpFile.deleteOnExit();
            count++;
        }

        return new File(path);
    }

    private String extractCore(File file, String saveDir) {
        try {
            String resultFilePath = "";
            if (!isTerminal)
                System.out.println("LOG: " + DATE_FORMAT.format(new Date().getTime()) + " | Unpack " + file.getPath() + " to catalog " + saveDir);
            String line;
            Process process;
            ProcessBuilder builder = new ProcessBuilder();
            builder.command("bash", "-c", "7z l -slt -ba " + file.getPath());
            builder.redirectErrorStream(true);
            process = builder.start();
            process.waitFor();
            BufferedReader br1 = new BufferedReader(new InputStreamReader(process.getInputStream(), "UTF-8"));//windows CP866
            while (true) {
                line = br1.readLine();
                if (line == null) {
                    break;
                }
                if (line.startsWith("Path")) {
                    resultFilePath = saveDir + "/" + line.substring(7);
                }
            }

            ProcessBuilder builder2 = new ProcessBuilder();
            builder2.redirectErrorStream(true);
            builder2.command("bash", "-c", "7z e " + file.getPath() + " -o" + saveDir + " -y");
            Process process2 = builder2.start();
            process2.waitFor();

            return resultFilePath;
        } catch (Exception e) {
            return "";
        }
    }

    //Перебор CSV
    public Object[] parseCSV(File file, String outputDir) {
        int total = 0;
        int error = 0;
        int normal = 0;
        if (!isTerminal)
            System.out.println("LOG: " + DATE_FORMAT.format(new Date().getTime()) + " | Parse " + file.getPath());
        //чтение
        CsvParserSettings readSettings = new CsvParserSettings();
        readSettings.getFormat().setLineSeparator("\n");
        //readSettings.getFormat().setQuote('"');
        //readSettings.getFormat().setDelimiter(',');
        readSettings.setHeaderExtractionEnabled(true);
        readSettings.setMaxCharsPerColumn(3000000);
        //запись
        CsvWriterSettings writerSettings = new CsvWriterSettings();
        writerSettings.setQuoteAllFields(true);
        //writerSettings.getFormat().setLineSeparator("\n");
        //writerSettings.getFormat().setQuote('"');
        //writerSettings.getFormat().setDelimiter(',');

        CsvParser parser = new CsvParser(readSettings);
        parser.beginParsing(file, "UTF-8");
        String[] headers = parser.getRecordMetadata().headers();

        int headersLength = headers.length;
        File fileNewCSV = new File(outputDir + "/" + file.getName() + ".NEW.csv");
        CsvWriter writer = new CsvWriter(fileNewCSV, "UTF-8", writerSettings);
        writer.writeHeaders(headers);

        String[] row;
        while ((row = parser.parseNext()) != null) {
            total++;
            if (row.length == headersLength) {
                normal++;
                writer.writeRow(row);
            } else {
                error++;
            }
        }
        parser.stopParsing();
        writer.close();

        Object[] objects = new Object[4];
        objects[0] = fileNewCSV;
        objects[1] = total;
        objects[2] = normal;
        objects[3] = error;
        return objects;
    }

    public boolean connectToDB(String connectionString, String user, String pass) {
        boolean res = false;
        try {
            Class.forName("org.postgresql.Driver");
            con = DriverManager.getConnection("jdbc:postgresql://" + connectionString, user, pass);
            con.setAutoCommit(true);
            res = true;
        } catch (SQLException | ClassNotFoundException e) {
            if (!isTerminal)
                System.out.println("LOG: " + DATE_FORMAT.format(new Date().getTime()) + " | Database connection error " + connectionString + " " + user + " " + pass);
        }
        return res;
    }

    public long importToBase(File fileNewCSV, String outputDirInDocker, String tableName) {
        long size = -1L;
        try {
            try (Statement stmt = con.createStatement()) {
                stmt.execute(createFunction);
                String sql = "select * from public.import_csv('" + outputDirInDocker + "/" + fileNewCSV.getName() + "', '" + tableName + "');";
                stmt.execute(sql);
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    size = rs.getLong(1);
                }
                rs.close();
            }
            con.close();
        } catch (Exception e) {
            if (!isTerminal)
                System.out.println("LOG: " + DATE_FORMAT.format(new Date().getTime()) + " | Import error " + fileNewCSV.getPath() + ": " + e.getLocalizedMessage());
        }
        return size;
    }

    public Connection createConnection(String connectionString, String user, String pass) {
        Connection connection = null;
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection("jdbc:postgresql://" + connectionString, user, pass);
            connection.setAutoCommit(true);
        } catch (SQLException | ClassNotFoundException e) {
            if (!isTerminal)
                System.out.println("LOG: " + DATE_FORMAT.format(new Date().getTime()) + " | Database connection error " + connectionString + " " + user + " " + pass);
        }
        return connection;
    }
}
