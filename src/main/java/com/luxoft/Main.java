package com.luxoft;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class Main {
    public static void main(String[] args) {

        String catalog = "dir";
        File dir = new File(catalog);
        dir.mkdir();
        String fileName = catalog + "/test12.txt";

        byte[] bytes = new byte[] {'A', 'B', 'C', 'D', 'E', 'F'};
        String str = new String(bytes, StandardCharsets.UTF_8);
        int length = bytes.length;

        try (RandomAccessFile raf = new RandomAccessFile(fileName, "rw")) {
            FileChannel channel = raf.getChannel();
            MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_WRITE, 0, length);
            mbb.put(bytes);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }

        /*try { // don't work for bytes properly
            Files.writeString(new File(fileName).toPath(), str, StandardCharsets.UTF_8, StandardOpenOption.CREATE);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }*/

        /*try (FileWriter fw = new FileWriter(fileName)) {
            fw.write(str);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }*/

        /*try (FileOutputStream fos = new FileOutputStream(fileName)) {
            fos.write(bytes);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }*/

        /*try (RandomAccessFile raf = new RandomAccessFile(fileName, "rw")) {
                raf.write(bytes);
        } catch (IOException ex) {
            System.out.println(ex.getMessage());
        }*/
    }

}