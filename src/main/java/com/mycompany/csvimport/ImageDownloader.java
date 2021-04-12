package com.mycompany.csvimport;

import net.coobird.thumbnailator.Thumbnails;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Скачивает и скалирует изображения
 */
public class ImageDownloader {

    private final Util UTIL = new Util();

    private final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public final int DEFAULT_BUFFER_SIZE = 8192;
    public final String ANSI_RESET = "\u001B[0m";
    public final String ANSI_BLACK = "\u001B[30m";
    public final String ANSI_RED = "\u001B[31m";
    public final String ANSI_GREEN = "\u001B[32m";
    public final String ANSI_YELLOW = "\u001B[33m";
    public final String ANSI_BLUE = "\u001B[34m";
    public final String ANSI_PURPLE = "\u001B[35m";
    public final String ANSI_CYAN = "\u001B[36m";
    public final String ANSI_WHITE = "\u001B[37m";
    private final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    private int numberThreads;
    private String tableName;
    private int numberRows;
    private Connection connection;
    private String targetFolder;

    public ImageDownloader(Connection connection, String tableName, int numberRows, int numberThreads, String targetFolder) {
        this.connection = connection;
        this.tableName = tableName;
        this.numberRows = numberRows;
        this.numberThreads = numberThreads;
        this.targetFolder = targetFolder;
    }

    public void start() {
        Map<Integer, String> sqlMap = new HashMap<>();

        if (numberRows >= numberThreads) {
            long div = numberRows / numberThreads;
            long mod = numberRows % numberThreads;

            for (int i = 0; i < numberThreads; i++) {
                long offset = div * i;
                if (i == numberThreads - 1) div = div + mod;

                String limit = "limit " + div + " offset " + offset;
                sqlMap.put(i,
                        "SELECT id, unical_id, (string_to_array(images, ',')) FROM products  WHERE images is not null and images != '' order by unical_id " + limit);
//                sqlMap.put(i,
//                        "SELECT id, unical_id, (string_to_array(images, ',')) FROM products  WHERE images is not null and images != '' and array_length(string_to_array(images, ','), 1) > 2 order by unical_id " + limit);
                System.out.println(sqlMap.get(i));
            }
        }
        ExecutorService executor = Executors.newFixedThreadPool(numberThreads);
        for (Map.Entry<Integer, String> sql : sqlMap.entrySet()) {

            executor.execute(() -> {
                try {
                    OkHttpClient.Builder builder = new OkHttpClient.Builder();
                    builder = configureToIgnoreCertificate(builder);
                    OkHttpClient client = builder.connectTimeout(10, TimeUnit.SECONDS).readTimeout(10, TimeUnit.SECONDS).build();
                    extract(sql.getValue(), "Поток " + sql.getKey() + ": ", client);
                } catch (SQLException tr) {
                    System.err.println("Поток " + sql.getKey() + ": " + tr.getMessage());
                }
            });
        }
        executor.shutdown();
    }

    private String extract(String sql, String threadName, OkHttpClient client) throws SQLException {
        int bad = 0;
        int success = 0;
        int total = 0;

        long startTime = System.currentTimeMillis();
        Connection connection = UTIL.createConnection("95.111.236.242/murad", "murat", "Murat2021#");
        Statement st = connection.createStatement();
        st.setFetchSize(250);
        ResultSet rs = st.executeQuery(sql);

        while (rs.next()) {
            long startTimeOne = System.currentTimeMillis();
            String color = ANSI_GREEN;
            String unical_id = rs.getString(2);

            try {
                String images[] = (String[]) rs.getArray(3).getArray();
                if (images.length > 0) {
                    if (downloadAndScale(images, unical_id, client)) {
                        success++;
                    } else {
                        throw new Exception("Не удалось скачать ни один файл из продукта: " + unical_id);
                    }
                } else {
                    throw new Exception("Нет ни одной картинки у продукта: " + unical_id);
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
                color = ANSI_RED;
                bad++;
            } finally {
                long endTimeOne = System.currentTimeMillis();
                long durationOne = (endTimeOne - startTimeOne);
                System.out.println(color + threadName + total + " " + unical_id + " " + durationOne + " ms" + " / " + durationOne / 1000 % 60 + " s" + ANSI_RESET);
                total++;
            }
        }

        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime);
        st.close();
        connection.close();
        String time = String.format("%02d:%02d:%02d", duration / 1000 / 3600, duration / 1000 / 60 % 60, duration / 1000 % 60);
        String res = threadName + "Всего затрачено: " + time + " пропущено " + bad + " сконвертированно " + success;
        rs.close();

        return res;
    }

    private boolean downloadAndScale(String[] images, String unical_id, OkHttpClient client) {
        for (String imageUrl : images) {
            String fileImage = unical_id + "_" + imageUrl.substring(imageUrl.lastIndexOf("/") + 1);
            Request request = new Request.Builder().url(imageUrl)
                    .addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36")
                    .build();
            try {
                try (Response response = client.newCall(request).execute()) {
                    try (InputStream in = response.body().byteStream()) {
                        Thumbnails.of(in)
                                .size(360, 360).keepAspectRatio(true)
                                .outputQuality(0.8)
                                .toFile(targetFolder + fileImage);
                    }
                }
            } catch (Exception e) {
                continue; //неудача переходим к следующему
            }
            break; //скачали хотя бы одну выходим из цикла
        }
        return true;
    }

    private OkHttpClient.Builder configureToIgnoreCertificate(OkHttpClient.Builder builder) {
        try {
            // Create a trust manager that does not validate certificate chains
            final TrustManager[] trustAllCerts = new TrustManager[]{
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                        }

                        @Override
                        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) {
                        }

                        @Override
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return new java.security.cert.X509Certificate[]{};
                        }
                    }
            };

            // Install the all-trusting trust manager
            final SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            // Create an ssl socket factory with our all-trusting manager
            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

            builder.sslSocketFactory(sslSocketFactory, (X509TrustManager) trustAllCerts[0]);
            builder.hostnameVerifier((hostname, session) -> true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return builder;
    }


}
