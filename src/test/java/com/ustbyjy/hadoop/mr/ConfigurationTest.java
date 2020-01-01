package com.ustbyjy.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConfigurationTest {

    @Test
    public void testGetProperty() {
        Configuration conf = new Configuration();
        conf.addResource("configuration-1.xml");

        assertThat(conf.get("color"), is("yellow"));
        assertThat(conf.getInt("size", 0), is(10));
        assertThat(conf.get("breadth", "wide"), is("wide"));
    }

    @Test
    public void testMergeResources() {
        Configuration conf = new Configuration();
        conf.addResource("configuration-1.xml");
        conf.addResource("configuration-2.xml");

        assertThat(conf.getInt("size", 0), is(12));
        // 覆盖final属性通常意味着配置错误，最后会弹出警告消息来帮助进行故障诊断
        assertThat(conf.get("weight"), is("heavy"));
    }

    @Test
    public void testVariable() {
        Configuration conf = new Configuration();
        conf.addResource("configuration-1.xml");
        conf.addResource("configuration-2.xml");

        assertThat(conf.get("size-weight"), is("12,heavy"));

        System.setProperty("size", "14");
        assertThat(conf.get("size-weight"), is("14,heavy"));
    }

}
