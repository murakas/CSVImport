package com.mycompany.csvimport;

import com.google.gson.*;
import com.mycompany.csvimport.model.DataBaseSettings;

import java.io.File;

import java.sql.*;
import java.util.*;


public class MainTest {

    public static void main(String[] args) {
        Gson GSON = new Gson();
        Util UTIL = new Util(true);


        Connection conn = UTIL.createConnection("localhost:5432/test", "postgres", "168");
        try {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                Integer k = stmt.executeUpdate("insert into test_table_1 (name) values('Emin')");

                System.out.printf("%d row(s) updated!", k);
                conn.commit();
            }
            conn.close();
        } catch (Exception e) {
            System.err.println(e);
        }


        //2-Формируем запрос

        File myObj = new File("C:\\Users\\Murad\\Downloads\\import_productive_webgains.json");
        StringBuilder data = new StringBuilder();

        try {
            Scanner myReader = new Scanner(myObj);

            while (myReader.hasNextLine()) {
                data.append(myReader.nextLine());
            }
            myReader.close();
        } catch (Exception e) {
        }
        JsonElement element = JsonParser.parseString(data.toString());

        JsonObject jsonObjConnTarget = element.getAsJsonObject().getAsJsonObject("dbTarget");
        JsonObject jsonObjConnSource = element.getAsJsonObject().getAsJsonObject("dbSource");

        /**
         ***** ПОДГОТАВЛИВАЕМ БАЗУ *****
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
            JsonElement jsonElement = entry.getValue().getAsJsonObject().get("columnsSource");
            if (!jsonElement.isJsonNull()) {
                JsonArray array = jsonElement.getAsJsonArray();
                String[] strings = new String[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    strings[i] = array.get(i).getAsString();
                }
                String joinedInsert;
                if (strings.length > 1) {
                    joinedInsert = "concat_ws(',', " + String.join(",", strings) + ") as c" + c;
                } else {
                    String distinct = "";
                    if (entry.getKey().equals("unical_id"))
                        distinct = "distinct on (" + String.join(",", strings) + ") ";

                    joinedInsert = distinct + "(" + String.join(",", strings) + ") as c" + c;
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

        for (String s : updateColumns.keySet()) {
            System.out.println(s);
        }

        String targetColumns4Update = String.join(", ", updateColumns.keySet().toArray(new String[0]));
        String sourceColumns4Update = String.join(", ", updateColumns.values().toArray(new String[0]));

        String targetColumns4Insert = String.join(", ", insertColumns.keySet().toArray(new String[0]));
        String sourceColumns4Insert = String.join(", ", insertColumns.values().toArray(new String[0]));

        System.out.println(targetColumns4Insert);
        System.out.println(sourceColumns4Insert);

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
        //Подключаемся к базе target и выполняем запрос
        System.out.println(slqDeleteProducts);

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
        /*Connection connTarget = UTIL.createConnection(
                jsonObjConnTarget.get("serverIp").getAsString() + "/" + jsonObjConnTarget.get("dataBase").getAsString(),
                jsonObjConnTarget.get("user").getAsString(),
                jsonObjConnTarget.get("password").getAsString());
        try {
            connTarget.setAutoCommit(false);
            try (Statement stmt = connTarget.createStatement()) {
                stmt.execute(sqlTgrInstallPostgres_fdw);
                stmt.execute(sqlTgrCreateServer);
                stmt.execute(sqlTgrCreateMapping);
                stmt.execute(sqlTgrImportPublicScheme);

                connTarget.commit();
            }
            connTarget.close();
        } catch (Exception e) {
            System.err.println(e);
        }*/
    }
//    public static void main(String[] args) throws SQLException {
//        long startTime = System.currentTimeMillis();
//        Util util = new Util(false);
//        Connection conn = util.createConnection("192.168.35.89:2345/test", "postgres", "168");
//        Connection conn1 = util.createConnection("192.168.35.89:2345/test", "postgres", "168");
//
//        conn.setAutoCommit(false);
//        conn1.setAutoCommit(false);
//
//        Statement st = conn.createStatement();
//
//        st.setFetchSize(5000);
//
//        ResultSet rs = st.executeQuery("SELECT product_id FROM table2");
//        ResultSet rs1 = new CachedRowSetImpl();
//        PreparedStatement statement = conn1.prepareStatement("SELECT count(product_id) FROM table0 t where t.product_id = ? limit 1");
//
//        String product_id = "";
//        int count = 0;
//
//        while (rs.next()) {
//            product_id = rs.getString(1);
//            statement.setString(1, product_id);
//            rs1 = statement.executeQuery();
//            while (rs1.next()) {
//                if (rs1.getInt(1) == 0)
//                    System.out.println(count + " - " + product_id);
//            }
//            count++;
//        }
//        rs1.close();
//        statement.close();
//        conn1.close();
//
//        rs.close();
//        st.close();
//        conn.close();
//
//        long endTime = System.currentTimeMillis();
//        long duration = (endTime - startTime);
//        String time = String.format("%02d:%02d:%02d", duration / 1000 / 3600, duration / 1000 / 60 % 60, duration / 1000 % 60);
//        System.out.println(time);
//    }
//    public static void main(String[] args) {
//        Gson gson = new Gson();
//
//        try {
//            File myObj = new File("C:\\Users\\Murad\\Downloads\\import_productive.json");
//            Scanner myReader = new Scanner(myObj);
//            StringBuilder data = new StringBuilder();
//            while (myReader.hasNextLine()) {
//                data.append(myReader.nextLine());
//            }
//            myReader.close();
//
//            JsonElement element = JsonParser.parseString(data.toString());
//
//            JsonObject obj = element.getAsJsonObject().getAsJsonObject("columns");
//            Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();
//            LinkedHashMap<String, String> compareColumns = new LinkedHashMap<>();
//
//            for (Map.Entry<String, JsonElement> entry : entries) {
//                boolean isComparisonField = entry.getValue().getAsJsonObject().get("comparisonField").getAsBoolean();
//                //сравниваемые поля
//                if (isComparisonField) {
//                    compareColumns.put(entry.getKey(), entry.getValue().getAsJsonObject().get("columnsSource").getAsString());
//                }
//            }
//            String advertiser = element.getAsJsonObject().get("advertiser").getAsString() + "_" + element.getAsJsonObject().get("datafeedId").getAsString();
//
//
//            DataBaseSettings sourceDB = gson.fromJson(element.getAsJsonObject().get("dbSource").getAsJsonObject(), DataBaseSettings.class);
//            DataBaseSettings targetDB = gson.fromJson(element.getAsJsonObject().get("dbTarget").getAsJsonObject(), DataBaseSettings.class);
//
//            String deleteSql = "delete from %s t where t.id in (select t1.id from %s t1 left join %s t2 on t1.unical_id != t2.%s where t1.advertiser = '%s')";
//
//            //1. Удаление
//            String targetColumn = compareColumns.entrySet().iterator().next().getValue();
//            deleteSql = String.format(deleteSql,
//                    targetDB.getTableName(),
//                    targetDB.getTableName(),
//                    sourceDB.getTableName(),
//                    targetColumn,
//                    advertiser);
//            //Подключаемся к базе target и выполняем запрос
//            System.out.println(deleteSql);
//        } catch (FileNotFoundException e) {
//            System.out.println("An error occurred.");
//            e.printStackTrace();
//        }
//    }
}
