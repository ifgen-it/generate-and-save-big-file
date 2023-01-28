package com.luxoft;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class Main {
    public static void main(String[] args) {

        String catalog = "dir";
        File dir = new File(catalog);
        dir.mkdir();
        String fileName = catalog + "/test12.txt";

        try (FileOutputStream fos = new FileOutputStream(fileName)) {
            String str = "hello111";
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            fos.write(bytes);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }

        /*try (RandomAccessFile raf = new RandomAccessFile(fileName, "rw")) {
            try {
                String str = "hello";
                byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
                raf.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }*/
    }

}