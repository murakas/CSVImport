package com.mycompany.csvimport.controller;

import com.google.gson.*;
import com.mycompany.csvimport.Util;
import com.mycompany.csvimport.model.DataBaseSettings;
import com.mycompany.csvimport.model.response.Response;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;


@Path("/")
public class MainController {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final Util UTIL = new Util();

    @POST
    @Path("/import")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public String importCSV(String importJson) {
        long startTime = System.currentTimeMillis();

        JsonElement element = JsonParser.parseString(importJson);

        String inputFile = element.getAsJsonObject().get("inputFile").getAsString();
        String outputDir = element.getAsJsonObject().get("outputDir").getAsString();
        String outputDirInDocker = outputDir; //пока без docker
        String connectionString = element.getAsJsonObject().get("connectionString").getAsString();
        String user = element.getAsJsonObject().get("user").getAsString();
        String pass = element.getAsJsonObject().get("pass").getAsString();
        String tableName = element.getAsJsonObject().get("tableName").getAsString();

        String timestamp_start = DATE_FORMAT.format(new Date().getTime());


        System.out.println("LOG: " + timestamp_start + " | Start");

        if (!UTIL.connectToDB(connectionString, user, pass)) {
            return printErrorResponseMessage("Error. Failed to connect to Postgres server!");
        }

        //Если файл из интернета
        Pattern patternHttp = Pattern.compile("^(https?://)");
        if (patternHttp.matcher(inputFile).find()) {
//            //Качаем файл
            try {
                File file = UTIL.downloadFile(inputFile, outputDir);
                if (file == null || !file.exists())
                    throw new IOException("Unknown error while trying to download " + inputFile);
                inputFile = file.getPath();
            } catch (IOException e) {
                System.out.println("LOG: " + DATE_FORMAT.format(new Date().getTime()) + " | " + e.getMessage());
                return printErrorResponseMessage("Error. Failed to unpack file " + inputFile + " | " + e.getMessage());
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
                return printErrorResponseMessage("Error. Failed to unpack file " + inputFile);
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
                return printErrorResponseMessage("Error. Failed to parse file " + inputFile);
            }
        }

        //Парсинг json

        //Импорируем
        File fileCSVImport = new File(inputFile);
        long size = UTIL.importToBase(fileCSVImport, outputDirInDocker, tableName);
        fileCSVImport.deleteOnExit();
        if (size < 0) return printErrorResponseMessage("Error. Failed to import file " + inputFile);

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
        String res = GSON.toJson(response);
        System.out.println(res);
        return res;
    }

    public String printErrorResponseMessage(String message) {
        Response response = new Response();
        response.setStatus("error");
        response.setMessage(message);
        String res = GSON.toJson(response);
        System.out.println(res);
        return res;
    }

    @POST
    @Path("/update")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public String update(String updateJson) {
        long startTime = System.currentTimeMillis();

        JsonElement element = JsonParser.parseString(updateJson);
        JsonObject jsonObjConnTarget = element.getAsJsonObject().getAsJsonObject("dbTarget");
        JsonObject jsonObjConnSource = element.getAsJsonObject().getAsJsonObject("dbSource");
        /**
         ***** ПОДГОТАВЛИВАЕМ БАЗУ ИСТОЧНИК *****
         */
        String srcServerIp = jsonObjConnSource.get("serverIp").getAsString();
        String srcHost = srcServerIp.substring(0, srcServerIp.indexOf(":"));
        String srcPort = srcServerIp.substring(srcServerIp.indexOf(":") + 1);
        String srcDbName = jsonObjConnSource.get("dataBase").getAsString();
        String srcUser = jsonObjConnSource.get("user").getAsString();
        String srcPassword = jsonObjConnSource.get("password").getAsString();
        String srcTableName = jsonObjConnSource.get("tableName").getAsString();

        //Установка расширения postgres_fdw
        String sqlTgrInstallPostgres_fdw = "CREATE EXTENSION if not exists postgres_fdw;";
        //Создание подключения к сторонему серверу
        String sqlTgrCreateServer = String.format("CREATE SERVER if not exists csv_import FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '%s', port '%s', dbname '%s');",
                srcHost, srcPort, srcDbName);
        //Сопоставление пользователей
        String sqlTgrCreateMapping = String.format("CREATE USER mapping if not exists FOR %s SERVER csv_import OPTIONS (user '%s', password '%s');",
                srcUser, srcUser, srcPassword);
        //Создание сторонней таблицы
        String sqlTgrImportPublicScheme = String.format("IMPORT FOREIGN SCHEMA public LIMIT TO (%s) from SERVER csv_import into public;", srcTableName);
        //Удаление сторонней таблицы
        String sqlTgrDropForeignTable = String.format("drop foreign table if exists %s", srcTableName);
        //Удаление таблицы source
        String sqlTgrDropSourceTable = String.format("drop table if exists %s", srcTableName);

        /**
         ***** ФОРМИРУЕМ ЗАПРОСЫ delete, insert, update *****
         */
        JsonObject obj = element.getAsJsonObject().getAsJsonObject("columns");
        Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();

        LinkedHashMap<String, String> insertColumns = new LinkedHashMap<>();
        LinkedHashMap<String, String> updateColumns = new LinkedHashMap<>();
        LinkedHashMap<String, String> compareColumns = new LinkedHashMap<>();

        int c = 1;
        //список полей
        for (Map.Entry<String, JsonElement> entry : entries) {

            boolean isUpdate = entry.getValue().getAsJsonObject().get("update").getAsBoolean();
            boolean isComparisonField = entry.getValue().getAsJsonObject().get("comparisonField").getAsBoolean();

            //поля которые нужно обновить
            JsonElement columnsSource = entry.getValue().getAsJsonObject().get("columnsSource");
            if (!columnsSource.isJsonNull()) {
                JsonArray arrayColumnsSource = columnsSource.getAsJsonArray();
                String[] columnStrings = new String[arrayColumnsSource.size()];
                for (int i = 0; i < arrayColumnsSource.size(); i++) {
                    columnStrings[i] = arrayColumnsSource.get(i).getAsString();
                }
                String joinedInsert;
                if (columnStrings.length > 1) {
                    joinedInsert = "concat_ws(',', " + String.join(",", columnStrings) + ") as c" + c;
                } else {
                    String distinct = "";
                    if (entry.getKey().equals("unical_id"))
                        distinct = "distinct on (" + String.join(",", columnStrings) + ") ";

                    joinedInsert = distinct + "(" + String.join(",", columnStrings) + ") as c" + c;
                }
                insertColumns.put(entry.getKey(), joinedInsert); //поля

                if (isUpdate) {
                    updateColumns.put(entry.getKey(), "excluded." + entry.getKey()); //поля
                }
                c++;
            }

            //сравниваемые поля
            if (isComparisonField && !entry.getValue().getAsJsonObject().get("columnsSource").isJsonNull()) {
                compareColumns.put(entry.getKey(), entry.getValue().getAsJsonObject().get("columnsSource").getAsString());
            }
        }

        String advertiser = element.getAsJsonObject().get("advertiser").getAsString() + "_" + element.getAsJsonObject().get("datafeedId").getAsString();
        insertColumns.put("advertiser", "'" + advertiser + "' as " + "c" + c);

        String targetColumns4Update = String.join(", ", updateColumns.keySet().toArray(new String[0]));
        String sourceColumns4Update = String.join(", ", updateColumns.values().toArray(new String[0]));

        String targetColumns4Insert = String.join(", ", insertColumns.keySet().toArray(new String[0]));
        String sourceColumns4Insert = String.join(", ", insertColumns.values().toArray(new String[0]));

        DataBaseSettings sourceDataBaseSettings = GSON.fromJson(element.getAsJsonObject().get("dbSource").getAsJsonObject(), DataBaseSettings.class);
        DataBaseSettings targetDataBaseSettings = GSON.fromJson(element.getAsJsonObject().get("dbTarget").getAsJsonObject(), DataBaseSettings.class);

        String targetColumn = "";
        if (compareColumns.entrySet().iterator().hasNext())
            targetColumn = compareColumns.entrySet().iterator().next().getValue();

        String slqDeleteProducts = String.format("delete from %s t1 where t1.unical_id not in (select t2.%s from %s t2) and t1.advertiser = '%s'",
                targetDataBaseSettings.getTableName(),
                targetColumn,
                sourceDataBaseSettings.getTableName(),
                advertiser);

        String sqlInsertOrUpdateProducts = String.format("insert into %s(%s) select %s from %s on conflict (unical_id, advertiser) do update set (%s) = (%s)",
                targetDataBaseSettings.getTableName(),
                targetColumns4Insert,
                sourceColumns4Insert,
                sourceDataBaseSettings.getTableName(),
                targetColumns4Update,
                sourceColumns4Update);

        System.out.println(slqDeleteProducts);
        System.out.println(sqlInsertOrUpdateProducts);
        /**
         ***** ВЫПОЛНЯЕМ КОМАНДЫ В БАЗЕ *****
         */
        Connection connTarget = UTIL.createConnection(
                jsonObjConnTarget.get("serverIp").getAsString() + "/" + jsonObjConnTarget.get("dataBase").getAsString(),
                jsonObjConnTarget.get("user").getAsString(),
                jsonObjConnTarget.get("password").getAsString());

        Connection connSource = UTIL.createConnection(
                jsonObjConnSource.get("serverIp").getAsString() + "/" + jsonObjConnSource.get("dataBase").getAsString(),
                jsonObjConnSource.get("user").getAsString(),
                jsonObjConnSource.get("password").getAsString());

        if (connTarget == null) return printErrorResponseMessage("Error connect to target database");
        if (connSource == null) return printErrorResponseMessage("Error connect to source database");

        int rowDelete;
        int rowUpdate;
        try {
            connTarget.setAutoCommit(false);
            try (Statement stmt = connTarget.createStatement()) {
                //Выполнение скрипта
                URI uri = getClass().getResource("/update.sql").toURI();
                String content = new String(Files.readAllBytes(Paths.get(uri)));
                //stmt.execute(content);

                stmt.execute(sqlTgrInstallPostgres_fdw);
                stmt.execute(sqlTgrCreateServer);
                stmt.execute(sqlTgrCreateMapping);
                stmt.execute(sqlTgrImportPublicScheme);

                rowDelete = stmt.executeUpdate(slqDeleteProducts);
                rowUpdate = stmt.executeUpdate(sqlInsertOrUpdateProducts);

                stmt.execute(sqlTgrDropForeignTable);
                connTarget.commit();
            }
            connTarget.close();

        } catch (Exception e) {
            return printErrorResponseMessage("Error. Failed update: " + e);
        }

//        try {
//            //Удаляем таблицу источник
//            try (Statement stmt = connSource.createStatement()) {
//                stmt.execute(sqlTgrDropSourceTable);
//            }
//            connSource.close();
//        } catch (Exception e) {
//            return printErrorResponseMessage("Error. Failed to delete source table " + srcTableName + ": " + e);
//        }

        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime);
        String time = String.format("%02d:%02d:%02d", duration / 1000 / 3600, duration / 1000 / 60 % 60, duration / 1000 % 60);

        JsonObject resJson = new JsonObject();
        resJson.addProperty("status", "success");
        resJson.addProperty("runtime", time);
        resJson.addProperty("deleted_row", rowDelete);
        resJson.addProperty("updated_row", rowUpdate);

        return GSON.toJson(resJson);
    }
}