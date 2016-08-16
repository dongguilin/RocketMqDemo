package com.guilin.rocketmq.demo;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Created by Administrator on 2016/8/12.
 */
public class MyTest {

    @Test
    public void test1() throws IOException {
        String path = MyTest.class.getClassLoader().getResource("ip").getPath();

        Collection<File> collection = FileUtils.listFiles(new File(path), null, false);

        for (File file : collection) {
            System.out.println(file.getName());
            List<String> list = FileUtils.readLines(file);
            for (String str : list) {

                System.out.println(Arrays.toString(str.split("\t")));
            }
        }


    }

}
