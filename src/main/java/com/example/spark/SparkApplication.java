package com.example.spark;

import java.io.*;

public class SparkApplication {

    public static void main(String[] args) throws IOException {

        new S3Service().sparkLoad();  //
    }
}
