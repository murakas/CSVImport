package com.mycompany.csvimport;

import com.google.gson.Gson;
import com.mycompany.csvimport.model.response.Response;

import java.io.File;
import java.util.regex.Pattern;

public class Console {
    private static final Gson GSON = new Gson();
    private static final Util UTIL = new Util(true);

    public static void main(String[] args) {


        long startTime = System.currentTimeMillis();
        if (args.length < 7) {
            printErrorResponseMessage("Error. Not all arguments were specified!");
        }

        String inputFile = args[0];
        String outputDir = args[1];
        String outputDirInDocker = args[2];
        String connectionString = args[3];
        String user = args[4];
        String pass = args[5];
        String tableName = args[6];

        if (!UTIL.connectToDB(connectionString, user, pass)) {
            printErrorResponseMessage("Error. Failed to connect to Postgres server!");
        }

        //Если файл из интернета
        Pattern patternHttp = Pattern.compile("^(https?://)");
        if (patternHttp.matcher(inputFile).find()) {
            //Качаем файл
            File file = UTIL.getFile(inputFile, outputDir);
            //Проверка
            if (file != null && file.exists()) {
                inputFile = file.getPath();
            } else {
                printErrorResponseMessage("Error. Failed to download file " + inputFile);
            }
        }

        //Если файл архив
        Pattern patternArchive = Pattern.compile("(\\.(gz|zip|7z|Z|bz2|lz|lz4|lzma|lzo|xz|zst|tar|ar|zx|cbz)$)");
        if (patternArchive.matcher(inputFile).find()) {
            File file = UTIL.extract(inputFile, outputDir);
            //Проверка
            if (file != null && file.exists()) {
                inputFile = file.getPath();
            } else {
                printErrorResponseMessage("Error. Failed to unpack file " + inputFile);
            }
        }

        //Парсим CSV
        Integer total = 0;
        Integer error = 0;
        Integer normal = 0;
        Pattern patternCSV = Pattern.compile("(\\.csv)$");
        if (patternCSV.matcher(inputFile).find()) {
            File fileCSV = new File(inputFile);
            Object[] objects = UTIL.parseCSV(fileCSV, outputDir);
            File fileCSVNew = (File) objects[0];
            total = (Integer) objects[1];
            normal = (Integer) objects[2];
            error = (Integer) objects[3];

            if (fileCSVNew.exists()) {
                inputFile = fileCSVNew.getPath();
                fileCSV.deleteOnExit();
            } else {
                printErrorResponseMessage("Error. Failed to parse file " + inputFile);
            }
        }

        //Парсинг json

        //Импорируем
        File fileCSVImport = new File(inputFile);
        long size = UTIL.importToBase(fileCSVImport, outputDirInDocker, tableName);
        fileCSVImport.deleteOnExit();
        if (size < 0) printErrorResponseMessage("Error. Failed to import file " + inputFile);

        //Если все норм то выводим результат
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime);
        String time = String.format("%02d:%02d:%02d", duration / 1000 / 3600, duration / 1000 / 60 % 60, duration / 1000 % 60);

        Response response = new Response();
        response.setStatus("success");
        response.setMessage("ok");
        response.setRuntime(time);
        response.setTotal_items(total);
        response.setSuccessful_items(normal);
        response.setBad_items(error);
        response.setTable_size(size);

        System.out.println(GSON.toJson(response));
    }

    public static void printErrorResponseMessage(String message) {
        Response response = new Response();
        response.setStatus("error");
        response.setMessage(message);
        System.out.println(GSON.toJson(response));
        System.exit(1);
    }
}
