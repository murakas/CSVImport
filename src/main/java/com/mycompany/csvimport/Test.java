package com.mycompany.csvimport;

import com.google.gson.*;
import com.mycompany.csvimport.model.DataBaseSettings;

import java.io.*;

import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;


public class Test {

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


//    public static void main(String[] args) {
//
//
//        Gson GSON = new Gson();
//        Util UTIL = new Util(true);
//
//        File file = UTIL.downloadFile("http://content.webgains.com/affiliates/datafeed.html?action=download&campaign=1306705&feeds=15205,14735,6275,21485,451,6333,7603,1608,13265,6973,20725,20085,2839,14505,14495,21245,7469,19135,19075,16935,17625,7343,5243,4043,2135,8033,6507,7341,4267,7311,4299,8199,19385,6691,1682,6831,19355,15955,6443,8221,2995,1652,9465,19745,21675,6643,3467,1880,19765,397,7101,2562,2929,20455,7651,21185,7255,8167,13245,10765,1315,4211,6615,7941,618,6057,9325,20615,9055,7365&categories=1013,1127,1128,8532,8534,8531,8176,5714,8530,1130,5715,21601,5716,1129,1131,1141,1198,1199,20314,20313,2311,1200,1201,5814,1154,1155,1156,1159,1165,1166,1167,1168,1169,1172,1171,7720,1170,1173,5752,5753,1174,1175,1177,1178,1179,5811,16382,1184,8576,8256,7722,9607,1142,1151,1145,1147,1153,5751,19885,1202,1203,1207,1261,1262,1264,5981,9474,5984,5983,8302,1265,9476,1266,8614,1275,19887,1208,1209,5824,5825,1218,1215,1211,16804,1214,1219,5836,5839,5841,20330,1221,1502,9455,1504,1222,5968,1228,1237,5908,8755,1235,1236,5906,1233,1234,1257,1258,1260,8396,8395,1245,1253,5972,5974,1247,8372,9457,1248,8379,8381,8382,8377,8380,8378,9467,1249,9468,5966,8390,9470,5964,9469,8384,5967,5903,1255,1251,1239,1240,1241,1242,1285,1288,1290,1286,1217,1289,1287,5997,9478,5999,9477,5996,5993,1291,8356,1292,1293,1294,1297,1298,1296,1299,1310,1311,1312,1313,1326,1328,1329,1330,6213,1331,1336,1332,1333,1334,8263,1337,1338,1339,6256,1342,1343,1345,1347,1353,1354,1361,1364,1370,1372,1369,1382,1384,1386,1388,1392,1395,1402,1403,1406,1405,16650,8397,9554,6302,1404,1407,1408,1409,1410,1412,12969,1413,6305,6303,1414,1028,1034,5630,9605,9649,5626,1069,1070,5689,5690,1467,1471,1481,5622,5623,1029,1031,1032,1033,1035,1036,5642,1041,1042,1037,1043,1038,1045,1046,5649,5657,1047,1082,1053,1055,1054,17152,1056,1073,1074,1122,1083,1084,1057,1061,1059,1063,5683,5680,1060,1062,5679,1119,1086,1093,1087,1088,1089,1090,1091,1097,1100,1101,1111,5709,5691,1132,1133,7838,15673,16413,19873,19872,1140,5741,8177,8558,8556,8557,8710,1135,5730,5615,8572,20298,20307,1139,20312,1459,6312,6319,1461,1466,20318,20317,1014,1020,8255,1018,5619,1019,1022,1023,1024,16726,1025,1026,1027,1021,1468,1469,1470,1473,1475,1480,1482,1210,8943,450,452,455,15674,20327,453,20328,1465,462,463,1484,6326,8358,1486,1494,1495,9479,1497,9480,1498,19902,1499,1500,1501,1503,1505,6354,8842,20319,1506,1507,20320,1508,1509,1510,1511,19877,19875,19878,6336,1516,1513,6338,6339,6340,19884,8365,1512,1519,6341,1520,1521,1522,1523,1525,1528,6353,8270,1527,7725,1529,1524,1532,20326,1300,1301,1304,1308,1541,1547&fields=extended&fieldIds=description,image_url,deeplink,category_id,merchant_category,category_name,category_path,price,product_id,product_name,program_id,program_name,last_updated,expiry,age,promotion_details,display_price,Delivery_type,manufacturers_product_number,in_stock,best_sellers,payment_methods,image_large_url,image_thumbnail_url,unit,recommended_retail_price,european_article_number,Colour,used_price,gender,flavour,weight,base_price,size,voucher_code,voucher_discount,manufacturer,related_product_ids,ISBN,image_url,keywords,short_description,stock_level,stock_level_date,country,delivery_period,brand,Fabric,merchant_category_id,metal,normal_price,voucher_price,seals,embargo,language,stock_code,barcode,type,upc,delivery_cost,ship_to,additionalproductdetails_1,additionalproductdetails_2,additionalproductdetails_3,currency,destination,condition,additional_delivery_cost_1,additional_delivery_cost_2,additional_delivery_cost_3,additionalproductdetails,additional_delivery_period_1,additional_image_2,additional_image_3,additional_thumb_2,additional_thumb_3&format=csv&separator=comma&zipformat=none&stripNewlines=0&apikey=655b1a19c4b763234d376037f86d0349", "/home/murad/");
//
//        Connection conn = UTIL.createConnection("localhost:5432/test", "postgres", "168");
//        try {
//            conn.setAutoCommit(false);
//            try (Statement stmt = conn.createStatement()) {
//                Integer k = stmt.executeUpdate("insert into test_table_1 (name) values('Emin')");
//
//                System.out.printf("%d row(s) updated!", k);
//                conn.commit();
//            }
//            conn.close();
//        } catch (Exception e) {
//            System.err.println(e);
//        }
//
//
//        //2-Формируем запрос
//
//        File myObj = new File("C:\\Users\\Murad\\Downloads\\import_productive_webgains.json");
//        StringBuilder data = new StringBuilder();
//
//        try {
//            Scanner myReader = new Scanner(myObj);
//
//            while (myReader.hasNextLine()) {
//                data.append(myReader.nextLine());
//            }
//            myReader.close();
//        } catch (Exception e) {
//        }
//        JsonElement element = JsonParser.parseString(data.toString());
//
//        JsonObject jsonObjConnTarget = element.getAsJsonObject().getAsJsonObject("dbTarget");
//        JsonObject jsonObjConnSource = element.getAsJsonObject().getAsJsonObject("dbSource");
//
//        /**
//         ***** ПОДГОТАВЛИВАЕМ БАЗУ *****
//         */
//        String srcServerIp = jsonObjConnSource.get("serverIp").getAsString();
//        String srcHost = srcServerIp.substring(0, srcServerIp.indexOf(":"));
//        String srcPort = srcServerIp.substring(srcServerIp.indexOf(":") + 1);
//        String srcDbName = jsonObjConnSource.get("dataBase").getAsString();
//        String srcUser = jsonObjConnSource.get("user").getAsString();
//        String srcPassword = jsonObjConnSource.get("password").getAsString();
//        String srcTableName = jsonObjConnSource.get("tableName").getAsString();
//
//        //Установка расширения postgres_fdw
//        String sqlTgrInstallPostgres_fdw = "CREATE EXTENSION if not exists postgres_fdw;";
//        //Создание подключения к сторонему серверу
//        String sqlTgrCreateServer = String.format("CREATE SERVER if not exists csv_import FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '%s', port '%s', dbname '%s');",
//                srcHost, srcPort, srcDbName);
//        //Сопоставление пользователей
//        String sqlTgrCreateMapping = String.format("CREATE USER mapping if not exists FOR %s SERVER csv_import OPTIONS (user '%s', password '%s');",
//                srcUser, srcUser, srcPassword);
//        //Создание сторонней таблицы
//        String sqlTgrImportPublicScheme = String.format("IMPORT FOREIGN SCHEMA public LIMIT TO (%s) from SERVER csv_import into public;", srcTableName);
//        //Удаление сторонней таблицы
//        String sqlTgrDropForeignTable = String.format("drop foreign table if exists %s", srcTableName);
//
//        /**
//         ***** ФОРМИРУЕМ ЗАПРОСЫ delete, insert, update *****
//         */
//        JsonObject obj = element.getAsJsonObject().getAsJsonObject("columns");
//        Set<Map.Entry<String, JsonElement>> entries = obj.entrySet();
//
//        LinkedHashMap<String, String> insertColumns = new LinkedHashMap<>();
//        LinkedHashMap<String, String> updateColumns = new LinkedHashMap<>();
//        LinkedHashMap<String, String> compareColumns = new LinkedHashMap<>();
//
//        int c = 1;
//        //список полей
//        for (Map.Entry<String, JsonElement> entry : entries) {
//
//            boolean isUpdate = entry.getValue().getAsJsonObject().get("update").getAsBoolean();
//            boolean isComparisonField = entry.getValue().getAsJsonObject().get("comparisonField").getAsBoolean();
//
//            //поля которые нужно обновить
//            JsonElement jsonElement = entry.getValue().getAsJsonObject().get("columnsSource");
//            if (!jsonElement.isJsonNull()) {
//                JsonArray array = jsonElement.getAsJsonArray();
//                String[] strings = new String[array.size()];
//                for (int i = 0; i < array.size(); i++) {
//                    strings[i] = array.get(i).getAsString();
//                }
//                String joinedInsert;
//                if (strings.length > 1) {
//                    joinedInsert = "concat_ws(',', " + String.join(",", strings) + ") as c" + c;
//                } else {
//                    String distinct = "";
//                    if (entry.getKey().equals("unical_id"))
//                        distinct = "distinct on (" + String.join(",", strings) + ") ";
//
//                    joinedInsert = distinct + "(" + String.join(",", strings) + ") as c" + c;
//                }
//                insertColumns.put(entry.getKey(), joinedInsert); //поля
//
//                if (isUpdate) {
//                    updateColumns.put(entry.getKey(), "excluded." + entry.getKey()); //поля
//                }
//                c++;
//            }
//
//            //сравниваемые поля
//            if (isComparisonField && !entry.getValue().getAsJsonObject().get("columnsSource").isJsonNull()) {
//                compareColumns.put(entry.getKey(), entry.getValue().getAsJsonObject().get("columnsSource").getAsString());
//            }
//        }
//
//        String advertiser = element.getAsJsonObject().get("advertiser").getAsString() + "_" + element.getAsJsonObject().get("datafeedId").getAsString();
//        insertColumns.put("advertiser", "'" + advertiser + "' as " + "c" + c);
//
//        for (String s : updateColumns.keySet()) {
//            System.out.println(s);
//        }
//
//        String targetColumns4Update = String.join(", ", updateColumns.keySet().toArray(new String[0]));
//        String sourceColumns4Update = String.join(", ", updateColumns.values().toArray(new String[0]));
//
//        String targetColumns4Insert = String.join(", ", insertColumns.keySet().toArray(new String[0]));
//        String sourceColumns4Insert = String.join(", ", insertColumns.values().toArray(new String[0]));
//
//        System.out.println(targetColumns4Insert);
//        System.out.println(sourceColumns4Insert);
//
//        DataBaseSettings sourceDataBaseSettings = GSON.fromJson(element.getAsJsonObject().get("dbSource").getAsJsonObject(), DataBaseSettings.class);
//        DataBaseSettings targetDataBaseSettings = GSON.fromJson(element.getAsJsonObject().get("dbTarget").getAsJsonObject(), DataBaseSettings.class);
//
//        String targetColumn = "";
//        if (compareColumns.entrySet().iterator().hasNext())
//            targetColumn = compareColumns.entrySet().iterator().next().getValue();
//
//        String slqDeleteProducts = String.format("delete from %s t1 where t1.unical_id not in (select t2.%s from %s t2) and t1.advertiser = '%s'",
//                targetDataBaseSettings.getTableName(),
//                targetColumn,
//                sourceDataBaseSettings.getTableName(),
//                advertiser);
//        //Подключаемся к базе target и выполняем запрос
//        System.out.println(slqDeleteProducts);
//
//        String sqlInsertOrUpdateProducts = String.format("insert into %s(%s) select %s from %s on conflict (unical_id, advertiser) do update set (%s) = (%s)",
//                targetDataBaseSettings.getTableName(),
//                targetColumns4Insert,
//                sourceColumns4Insert,
//                sourceDataBaseSettings.getTableName(),
//                targetColumns4Update,
//                sourceColumns4Update);
//
//        System.out.println(slqDeleteProducts);
//        System.out.println(sqlInsertOrUpdateProducts);
//        /**
//         ***** ВЫПОЛНЯЕМ КОМАНДЫ В БАЗЕ *****
//         */
//        /*Connection connTarget = UTIL.createConnection(
//                jsonObjConnTarget.get("serverIp").getAsString() + "/" + jsonObjConnTarget.get("dataBase").getAsString(),
//                jsonObjConnTarget.get("user").getAsString(),
//                jsonObjConnTarget.get("password").getAsString());
//        try {
//            connTarget.setAutoCommit(false);
//            try (Statement stmt = connTarget.createStatement()) {
//                stmt.execute(sqlTgrInstallPostgres_fdw);
//                stmt.execute(sqlTgrCreateServer);
//                stmt.execute(sqlTgrCreateMapping);
//                stmt.execute(sqlTgrImportPublicScheme);
//
//                connTarget.commit();
//            }
//            connTarget.close();
//        } catch (Exception e) {
//            System.err.println(e);
//        }*/
//    }
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

    public static void main(String[] args) {


        InputStream is = Test.class.getClassLoader().getResourceAsStream("update.sql");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
        String text = bufferedReader.lines().collect(Collectors.joining());
        System.out.println(text);

//        String url = "https://www.webgains.com/datafeed.html?action=download&campaign=1306705&feeds=15205,14735,6275,21485,451,6333,7603,1608,13265,6973,20725,20085,2839,14505,14495,21245,7469,19135,19075,16935,17625,7343,5243,4043,2135,8033,6507,7341,4267,7311,4299,8199,19385,6691,1682,6831,19355,15955,6443,8221,2995,1652,9465,19745,21675,6643,3467,1880,19765,397,7101,2562,2929,20455,7651,21185,7255,8167,13245,10765,1315,4211,6615,7941,618,6057,9325,20615,9055,7365&categories=1013,1127,1128,8532,8534,8531,8176,5714,8530,1130,5715,21601,5716,1129,1131,1141,1198,1199,20314,20313,2311,1200,1201,5814,1154,1155,1156,1159,1165,1166,1167,1168,1169,1172,1171,7720,1170,1173,5752,5753,1174,1175,1177,1178,1179,5811,16382,1184,8576,8256,7722,9607,1142,1151,1145,1147,1153,5751,19885,1202,1203,1207,1261,1262,1264,5981,9474,5984,5983,8302,1265,9476,1266,8614,1275,19887,1208,1209,5824,5825,1218,1215,1211,16804,1214,1219,5836,5839,5841,20330,1221,1502,9455,1504,1222,5968,1228,1237,5908,8755,1235,1236,5906,1233,1234,1257,1258,1260,8396,8395,1245,1253,5972,5974,1247,8372,9457,1248,8379,8381,8382,8377,8380,8378,9467,1249,9468,5966,8390,9470,5964,9469,8384,5967,5903,1255,1251,1239,1240,1241,1242,1285,1288,1290,1286,1217,1289,1287,5997,9478,5999,9477,5996,5993,1291,8356,1292,1293,1294,1297,1298,1296,1299,1310,1311,1312,1313,1326,1328,1329,1330,6213,1331,1336,1332,1333,1334,8263,1337,1338,1339,6256,1342,1343,1345,1347,1353,1354,1361,1364,1370,1372,1369,1382,1384,1386,1388,1392,1395,1402,1403,1406,1405,16650,8397,9554,6302,1404,1407,1408,1409,1410,1412,12969,1413,6305,6303,1414,1028,1034,5630,9605,9649,5626,1069,1070,5689,5690,1467,1471,1481,5622,5623,1029,1031,1032,1033,1035,1036,5642,1041,1042,1037,1043,1038,1045,1046,5649,5657,1047,1082,1053,1055,1054,17152,1056,1073,1074,1122,1083,1084,1057,1061,1059,1063,5683,5680,1060,1062,5679,1119,1086,1093,1087,1088,1089,1090,1091,1097,1100,1101,1111,5709,5691,1132,1133,7838,15673,16413,19873,19872,1140,5741,8177,8558,8556,8557,8710,1135,5730,5615,8572,20298,20307,1139,20312,1459,6312,6319,1461,1466,20318,20317,1014,1020,8255,1018,5619,1019,1022,1023,1024,16726,1025,1026,1027,1021,1468,1469,1470,1473,1475,1480,1482,1210,8943,450,452,455,15674,20327,453,20328,1465,462,463,1484,6326,8358,1486,1494,1495,9479,1497,9480,1498,19902,1499,1500,1501,1503,1505,6354,8842,20319,1506,1507,20320,1508,1509,1510,1511,19877,19875,19878,6336,1516,1513,6338,6339,6340,19884,8365,1512,1519,6341,1520,1521,1522,1523,1525,1528,6353,8270,1527,7725,1529,1524,1532,20326,1300,1301,1304,1308,1541,1547&fields=extended&fieldIds=description,image_url,deeplink,category_id,merchant_category,category_name,category_path,price,product_id,product_name,program_id,program_name,last_updated,expiry,age,promotion_details,display_price,Delivery_type,manufacturers_product_number,in_stock,best_sellers,payment_methods,image_large_url,image_thumbnail_url,unit,recommended_retail_price,european_article_number,Colour,used_price,gender,flavour,weight,base_price,size,voucher_code,voucher_discount,manufacturer,related_product_ids,ISBN,image_url,keywords,short_description,stock_level,stock_level_date,country,delivery_period,brand,Fabric,merchant_category_id,metal,normal_price,voucher_price,seals,embargo,language,stock_code,barcode,type,upc,delivery_cost,ship_to,additionalproductdetails_1,additionalproductdetails_2,additionalproductdetails_3,currency,destination,condition,additional_delivery_cost_1,additional_delivery_cost_2,additional_delivery_cost_3,additionalproductdetails,additional_delivery_period_1,additional_image_2,additional_image_3,additional_thumb_2,additional_thumb_3&format=csv&separator=comma&zipformat=none&stripNewlines=0&apikey=655b1a19c4b763234d376037f86d0349";
//        String url2 = "http://content.webgains.com/affiliates/datafeed.html?action=download&campaign=1306705&feeds=15205,14735,6275,21485,451,6333,7603,1608,13265,6973,20725,20085,2839,14505,14495,21245,7469,19135,19075,16935,17625,7343,5243,4043,2135,8033,6507,7341,4267,7311,4299,8199,19385,6691,1682,6831,19355,15955,6443,8221,2995,1652,9465,19745,21675,6643,3467,1880,19765,397,7101,2562,2929,20455,7651,21185,7255,8167,13245,10765,1315,4211,6615,7941,618,6057,9325,20615,9055,7365&categories=1013,1127,1128,8532,8534,8531,8176,5714,8530,1130,5715,21601,5716,1129,1131,1141,1198,1199,20314,20313,2311,1200,1201,5814,1154,1155,1156,1159,1165,1166,1167,1168,1169,1172,1171,7720,1170,1173,5752,5753,1174,1175,1177,1178,1179,5811,16382,1184,8576,8256,7722,9607,1142,1151,1145,1147,1153,5751,19885,1202,1203,1207,1261,1262,1264,5981,9474,5984,5983,8302,1265,9476,1266,8614,1275,19887,1208,1209,5824,5825,1218,1215,1211,16804,1214,1219,5836,5839,5841,20330,1221,1502,9455,1504,1222,5968,1228,1237,5908,8755,1235,1236,5906,1233,1234,1257,1258,1260,8396,8395,1245,1253,5972,5974,1247,8372,9457,1248,8379,8381,8382,8377,8380,8378,9467,1249,9468,5966,8390,9470,5964,9469,8384,5967,5903,1255,1251,1239,1240,1241,1242,1285,1288,1290,1286,1217,1289,1287,5997,9478,5999,9477,5996,5993,1291,8356,1292,1293,1294,1297,1298,1296,1299,1310,1311,1312,1313,1326,1328,1329,1330,6213,1331,1336,1332,1333,1334,8263,1337,1338,1339,6256,1342,1343,1345,1347,1353,1354,1361,1364,1370,1372,1369,1382,1384,1386,1388,1392,1395,1402,1403,1406,1405,16650,8397,9554,6302,1404,1407,1408,1409,1410,1412,12969,1413,6305,6303,1414,1028,1034,5630,9605,9649,5626,1069,1070,5689,5690,1467,1471,1481,5622,5623,1029,1031,1032,1033,1035,1036,5642,1041,1042,1037,1043,1038,1045,1046,5649,5657,1047,1082,1053,1055,1054,17152,1056,1073,1074,1122,1083,1084,1057,1061,1059,1063,5683,5680,1060,1062,5679,1119,1086,1093,1087,1088,1089,1090,1091,1097,1100,1101,1111,5709,5691,1132,1133,7838,15673,16413,19873,19872,1140,5741,8177,8558,8556,8557,8710,1135,5730,5615,8572,20298,20307,1139,20312,1459,6312,6319,1461,1466,20318,20317,1014,1020,8255,1018,5619,1019,1022,1023,1024,16726,1025,1026,1027,1021,1468,1469,1470,1473,1475,1480,1482,1210,8943,450,452,455,15674,20327,453,20328,1465,462,463,1484,6326,8358,1486,1494,1495,9479,1497,9480,1498,19902,1499,1500,1501,1503,1505,6354,8842,20319,1506,1507,20320,1508,1509,1510,1511,19877,19875,19878,6336,1516,1513,6338,6339,6340,19884,8365,1512,1519,6341,1520,1521,1522,1523,1525,1528,6353,8270,1527,7725,1529,1524,1532,20326,1300,1301,1304,1308,1541,1547&fields=extended&fieldIds=description,image_url,deeplink,category_id,merchant_category,category_name,category_path,price,product_id,product_name,program_id,program_name,last_updated,expiry,age,promotion_details,display_price,Delivery_type,manufacturers_product_number,in_stock,best_sellers,payment_methods,image_large_url,image_thumbnail_url,unit,recommended_retail_price,european_article_number,Colour,used_price,gender,flavour,weight,base_price,size,voucher_code,voucher_discount,manufacturer,related_product_ids,ISBN,image_url,keywords,short_description,stock_level,stock_level_date,country,delivery_period,brand,Fabric,merchant_category_id,metal,normal_price,voucher_price,seals,embargo,language,stock_code,barcode,type,upc,delivery_cost,ship_to,additionalproductdetails_1,additionalproductdetails_2,additionalproductdetails_3,currency,destination,condition,additional_delivery_cost_1,additional_delivery_cost_2,additional_delivery_cost_3,additionalproductdetails,additional_delivery_period_1,additional_image_2,additional_image_3,additional_thumb_2,additional_thumb_3&format=csv&separator=comma&zipformat=none&stripNewlines=0&apikey=655b1a19c4b763234d376037f86d0349";
//        try {
//            File file = getFile(url, "e:/");
//            if (file == null || !file.exists()) {
//                throw new IOException("Unknown error while trying to download " + url);
//            } ;
//            String inputFile = file.getPath();
//        } catch (IOException e) {
//            System.out.println("LOG: " + DATE_FORMAT.format(new Date().getTime()) + " | " + e.getMessage());
//        }
//        try {
//
//
//            URL obj = new URL(url);
//            HttpURLConnection conn = (HttpURLConnection) obj.openConnection();
//            conn.setReadTimeout(5000);
//            conn.addRequestProperty("Accept-Language", "en-US,en;q=0.8");
//            conn.addRequestProperty("User-Agent", "Mozilla");
//            conn.addRequestProperty("Referer", "google.com");
//
//            System.out.println("Request URL ... " + url);
//
//            boolean redirect = false;
//
//            // normally, 3xx is redirect
//            int status = conn.getResponseCode();
//            if (status != HttpURLConnection.HTTP_OK) {
//                if (status == HttpURLConnection.HTTP_MOVED_TEMP
//                        || status == HttpURLConnection.HTTP_MOVED_PERM
//                        || status == HttpURLConnection.HTTP_SEE_OTHER)
//                    redirect = true;
//            }
//
//            System.out.println("Response Code ... " + status);
//
//            if (redirect) {
//
//                // get redirect url from "location" header field
//                String newUrl = conn.getHeaderField("Location");
//
//                // get the cookie if need, for login
//                String cookies = conn.getHeaderField("Set-Cookie");
//
//                // open the new connnection again
//                conn = (HttpURLConnection) new URL(newUrl).openConnection();
//                conn.setRequestProperty("Cookie", cookies);
//                conn.addRequestProperty("Accept-Language", "en-US,en;q=0.8");
//                conn.addRequestProperty("User-Agent", "Mozilla");
//                conn.addRequestProperty("Referer", "google.com");
//
//                System.out.println("Redirect to URL : " + newUrl);
//
//            }
//
//            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
//            String inputLine;
//            StringBuffer html = new StringBuffer();
//
//            while ((inputLine = in.readLine()) != null) {
//                html.append(inputLine);
//            }
//            in.close();
//
//            System.out.println("URL Content... \n" + html.toString());
//            System.out.println("Done");
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }

    public static File getFile(String fileURL, String saveDir) throws IOException {
        File outputFile = null;
        URL url = new URL(fileURL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.addRequestProperty("User-Agent", "Mozilla");

        int status = -1; //по умолчанию ошибка

        int responseCode = conn.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK)
            status = 0;
        if (responseCode == HttpURLConnection.HTTP_MOVED_TEMP
                || responseCode == HttpURLConnection.HTTP_MOVED_PERM
                || responseCode == HttpURLConnection.HTTP_SEE_OTHER)
            status = 1;

        if (status == -1) {
            System.out.println("LOG: " + DATE_FORMAT.format(new Date().getTime()) + " | An error occurred while trying to download the file. Server returned HTTP code: " + responseCode);
            return null;
        }

        if (status == 1) {
            //делаем редирект
            String newUrl = conn.getHeaderField("Location");

            String cookies = conn.getHeaderField("Set-Cookie");

            conn = (HttpURLConnection) new URL(newUrl).openConnection();
            conn.setRequestProperty("Cookie", cookies);
            conn.addRequestProperty("User-Agent", "Mozilla");

            System.out.println("Redirect to URL : " + newUrl);

        }

        String fileName = null;
        String disposition = conn.getHeaderField("Content-Disposition");
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

        InputStream inputStream = conn.getInputStream();
        String saveFilePath = saveDir + File.separator + fileName;

        ReadableByteChannel channel = Channels.newChannel(inputStream);
        outputFile = new File(saveFilePath);
        FileOutputStream outputStream = new FileOutputStream(outputFile);
        outputStream.getChannel().transferFrom(channel, 0, Long.MAX_VALUE);
        outputStream.close();
        channel.close();
        inputStream.close();
        conn.disconnect();

        return outputFile;
    }
}
