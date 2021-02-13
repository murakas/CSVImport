package com.mycompany.csvimport;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Util {

    private final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private Connection connection;

    public File downloadFile(String fileURL, String saveDir) throws IOException {
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
        File outputFile = new File(saveFilePath);
        FileOutputStream outputStream = new FileOutputStream(outputFile);
        outputStream.getChannel().transferFrom(channel, 0, Long.MAX_VALUE);
        outputStream.close();
        channel.close();
        inputStream.close();
        conn.disconnect();

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
            connection = DriverManager.getConnection("jdbc:postgresql://" + connectionString, user, pass);
            connection.setAutoCommit(true);
            res = true;
        } catch (SQLException | ClassNotFoundException e) {
            System.out.println("LOG: " + DATE_FORMAT.format(new Date().getTime()) + " | Database connection error " + connectionString + " " + user + " " + pass);
        }
        return res;
    }

    public long importToBase(File fileNewCSV, String outputDirInDocker, String tableName) {
        long size = -1L;
        try {
            try (Statement stmt = connection.createStatement()) {
                InputStream is = MainController.class.getClassLoader().getResourceAsStream("import.sql");
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
                String content = bufferedReader.lines().collect(Collectors.joining("\n"));

                stmt.execute(content);

                String sql = "select * from public.import_csv('" + outputDirInDocker + "/" + fileNewCSV.getName() + "', '" + tableName + "');";
                stmt.execute(sql);
                ResultSet rs = stmt.executeQuery(sql);
                while (rs.next()) {
                    size = rs.getLong(1);
                }
                rs.close();
            }
            connection.close();
        } catch (Exception e) {
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
            System.out.println("LOG: " + DATE_FORMAT.format(new Date().getTime()) + " | Database connection error " + connectionString + " " + user + " " + pass);
        }
        return connection;
    }
}
