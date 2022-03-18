package com.example.spark;

import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import com.example.spark.*;

import java.io.*;
import java.util.*;

public class S3Service {


    public void sparkLoad() throws IOException {

        System.out.println("sparkLoad");

        long start = System.currentTimeMillis();
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("SparkRDS")
               // .master("local[5]")
                .config("spark.yarn.maxAppAttempts", "1")
                .config("spark.hadoop.fs.s3a.multipart.threshold", "2097152000")
                .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
                .config("spark.hadoop.fs.s3a.connection.maximum", "50000")
                .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
                .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
                .config("spark.hadoop.fs.s3a.access.key", "XX")
                .config("spark.hadoop.fs.s3a.secret.key", "XX")
                .getOrCreate();
        System.out.println("line 108");

        SQLContext sqlContext = sparkSession.sqlContext();
        System.out.println("line 112");

        Dataset<String> df = sqlContext.read().textFile("s3a://hp-bucket/zones/abc/test_file_hp.txt"); // fails at this line
       // Dataset<String> df = sqlContext.read().textFile("src/main/resources/test_file_hp.txt"); // fails at this line
       // Dataset<String> df = sqlContext.read().textFile("s3://sparkjob808/test_file_hp.txt");//s3://sparkjob808/test_file_hp.txt
        System.out.println("sqlContext created");

        Encoder<ABC> encoder = Encoders.bean(ABC.class);
        Dataset<ABC> df1 = df.map((MapFunction<String, ABC>) line -> {
            String[] values = line.split("\\|");
            ABC abc = new ABC(values[0], values[1], values[2], values[3], values[4], values[5]);
            return abc;
        }, encoder);

        System.out.println("created df1");

        df1.show();
        df1.collect();
////        /String url = "jdbc:mysql://abc-dev.net:3306/abc";
//        String table = "abc";
//
//        df1.write().mode(SaveMode.Overwrite).jdbc(url, table, properties());
        long end = System.currentTimeMillis();
        System.out.println(((end - start) / 1000) + " seconds" );
    }

    private static String parseFileName(String key) {
        String abc = "abc/";
        return key.substring((key.indexOf(abc) + abc.length() - 1) + 1, key.length());
    }

    public static Properties properties(){
        Properties prop = new Properties();
        prop.put("user", "xxxxxxxx");
        prop.put("password", "xxxxxxxxxx");
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return prop;
    }
}
