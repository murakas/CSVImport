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

public class TestCompressImage {


    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final int DEFAULT_BUFFER_SIZE = 8192;
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_BLACK = "\u001B[30m";
    public static final String ANSI_RED = "\u001B[31m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_WHITE = "\u001B[37m";
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();


    public static void main(String[] args) {
        Util util = new Util();
        long n = 0;
        int threadsCount = 20;

        try {
            Connection connection = util.createConnection("95.111.236.242:5432/murad", "murat", "Murat2021#");
            Statement st = connection.createStatement();
            st.setFetchSize(250);
            ResultSet rs = st.executeQuery("select count(id) from products");
            while (rs.next()) {
                n = rs.getLong(1);
            }
        } catch (Exception e) {
            System.err.println(e);
            System.exit(1);
        }

        Map<Integer, String> sqlMap = new HashMap<>();

        if (n >= threadsCount) {
            long div = n / threadsCount;
            long mod = n % threadsCount;

            for (int i = 0; i < threadsCount; i++) {
                long offset = div * i;
                if (i == threadsCount - 1) div = div + mod;

                String limit = "limit " + div + " offset " + offset;
                sqlMap.put(i,
                        "SELECT id, unical_id, (string_to_array(images, ','))[1] FROM products  WHERE images is not null and images != '' order by unical_id " + limit);
                System.out.println(sqlMap.get(i));

            }
        }

//
//        String sql1 = "SELECT id, unical_id, (string_to_array(images, ','))[1] FROM products  WHERE images is not null and images != '' order by unical_id limit 500";
//        String sql2 = "SELECT id, unical_id, (string_to_array(images, ','))[1] FROM products  WHERE images is not null and images != '' order by unical_id limit 500 offset 500";
        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);
        for (Map.Entry<Integer, String> sql : sqlMap.entrySet()) {
            executor.execute(() -> {
                try {
                    extract(sql.getValue(), "Поток " + sql.getKey() + ": ");
                } catch (SQLException tr) {
                    System.err.println("Поток " + sql.getKey() + ": " + tr.getMessage());
                }
            });
        }
        executor.shutdown();
    }

    private static void extract(String sql1, String threadName) throws SQLException {

        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        builder = configureToIgnoreCertificate(builder);
        final OkHttpClient client = builder.connectTimeout(10, TimeUnit.SECONDS).readTimeout(10, TimeUnit.SECONDS).build();

        Util u = new Util();
        Connection connection = u.createConnection("95.111.236.242:5432/murad", "murat", "Murat2021#");
        Statement st = connection.createStatement();
        st.setFetchSize(250);
        ResultSet rs = st.executeQuery(sql1);
        int a = 0;
        int b = 0;
        int c = 0;
        long startTime = System.currentTimeMillis();
        while (rs.next()) {
            long startTimeOne = System.currentTimeMillis();
            String color = "";
            String imageUrl = rs.getString(3);
            if (imageUrl != null && imageUrl.length() > 0) {
                try {
                    String fileImage = rs.getString(2) + "_" + imageUrl.substring(imageUrl.lastIndexOf("/") + 1);
                    Request request = new Request.Builder().url(imageUrl)
                            .addHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36")
                            .build();
                    try (Response response = client.newCall(request).execute()) {
                        try (InputStream in = response.body().byteStream()) {
                            Thumbnails.of(in)
                                    .size(360, 360).keepAspectRatio(true)
                                    .outputQuality(0.8)
                                    .toFile("C:/Users/Murad/Desktop/tmp/" + fileImage);
                        }
                    }
                    color = ANSI_GREEN;
                    b++;
                } catch (Exception e) {
                    System.err.println(e.getMessage() + ": " + imageUrl);
                    color = ANSI_RED;
                    a++;
                } finally {
                    long endTimeOne = System.currentTimeMillis();
                    long durationOne = (endTimeOne - startTimeOne);
                    System.out.println(color + threadName + c++ + " " + imageUrl + " " + durationOne + " ms" + " / " + durationOne / 1000 % 60 + " s" + ANSI_RESET);
                }
            }
        }
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime);
        String time = String.format("%02d:%02d:%02d", duration / 1000 / 3600, duration / 1000 / 60 % 60, duration / 1000 % 60);
        System.out.println(threadName + "Всего затрачено: " + time + " пропущено " + a + " сконвертированно " + b);
        rs.close();
        st.close();
        connection.close();
    }


    private static OkHttpClient.Builder configureToIgnoreCertificate(OkHttpClient.Builder builder) {
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
