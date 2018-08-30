/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.loader.test.functional;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.driver.TaskManager;
import com.baidu.hugegraph.loader.HugeGraphLoader;
import com.baidu.hugegraph.loader.exception.LoadException;
import com.baidu.hugegraph.loader.exception.ParseException;
import com.baidu.hugegraph.structure.constant.DataType;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.structure.schema.PropertyKey;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableList;

public class FileLoadTest {

    private static final Charset GBK = Charset.forName("GBK");
    private static final String CONFIG_PATH_PREFIX = "target/test-classes";
    private static String STORE_PATH_PREFIX;
    private static final String URL = "http://127.0.0.1:8080";
    private static final String GRAPH = "hugegraph";
    private static final HugeClient CLIENT = new HugeClient(URL, GRAPH);

    static {
        String path = "/profile.properties";
        // Read properties defined in maven profile
        try (InputStream is = FileLoadTest.class.getResourceAsStream(path)) {
            Properties properties = new Properties();
            properties.load(is);
            STORE_PATH_PREFIX = properties.getProperty("store_path");
        } catch (IOException e) {
            throw new RuntimeException(
                      "Failed to read properties defined in maven profile", e);
        }
    }

    @BeforeClass
    public static void setUp() throws IOException {
        clearFileData();
        clearServerData();
    }

    @Before
    public void init() {
    }

    @After
    public void clear() {
        clearFileData();
        clearServerData();
    }

    @AfterClass
    public static void tearDown() {
    }

    private static void clearFileData() {
        FileUtil.delete(storePath());
    }

    private static void clearServerData() {
        SchemaManager schema = CLIENT.schema();
        GraphManager graph = CLIENT.graph();
        TaskManager task = CLIENT.task();
        // Clear edge
        graph.listEdges().forEach(e -> graph.removeEdge(e.id()));
        // Clear vertex
        graph.listVertices().forEach(v -> graph.removeVertex(v.id()));

        // Clear schema
        List<Long> taskIds = new ArrayList<>();
        schema.getIndexLabels().forEach(il -> {
            taskIds.add(schema.removeIndexLabelAsync(il.name()));
        });
        taskIds.forEach(id -> task.waitUntilTaskCompleted(id, 5L));
        taskIds.clear();
        schema.getEdgeLabels().forEach(el -> {
            taskIds.add(schema.removeEdgeLabelAsync(el.name()));
        });
        taskIds.forEach(id -> task.waitUntilTaskCompleted(id, 5L));
        taskIds.clear();
        schema.getVertexLabels().forEach(vl -> {
            taskIds.add(schema.removeVertexLabelAsync(vl.name()));
        });
        taskIds.forEach(id -> task.waitUntilTaskCompleted(id, 5L));
        taskIds.clear();
        schema.getPropertyKeys().forEach(pk -> {
            schema.removePropertyKey(pk.name());
        });
    }

    private static String configPath(String fileName) {
        return Paths.get(CONFIG_PATH_PREFIX, fileName).toString();
    }

    private static String storePath(String... files) {
        return Paths.get(STORE_PATH_PREFIX, files).toString();
    }

    /**
     * NOTE: Unsupport auto create schema
     */
    //@Test
    public void testAutoCreateSchema() {
        String[] args = new String[]{"-f", "example/struct.json",
                                     "-g", GRAPH,
                                     "--num-threads", "2"};
        HugeGraphLoader.main(args);

        List<PropertyKey> propertyKeys = CLIENT.schema().getPropertyKeys();
        propertyKeys.forEach(pkey -> {
            Assert.assertEquals(DataType.TEXT, pkey.dataType());
        });

        List<Vertex> vertices = CLIENT.graph().listVertices();
        List<Edge> edges = CLIENT.graph().listEdges();

        Assert.assertEquals(7, vertices.size());
        Assert.assertEquals(6, edges.size());

        boolean interestedVertex = false;
        for (Vertex vertex : vertices) {
            Assert.assertEquals(String.class, vertex.id().getClass());
            if (((String) vertex.id()).contains("li,nary")) {
                interestedVertex = true;
                Assert.assertEquals("26", vertex.property("age"));
                Assert.assertEquals("Wu,han", vertex.property("city"));
            }
        }
        Assert.assertTrue(interestedVertex);

        boolean interestedEdge = false;
        for (Edge edge : edges) {
            Assert.assertEquals(String.class, edge.source().getClass());
            Assert.assertEquals(String.class, edge.target().getClass());
            if (((String) edge.source()).contains("marko") &&
                ((String) edge.target()).contains("vadas")) {
                interestedEdge = true;
                Assert.assertEquals("20160110", edge.property("date"));
                Assert.assertEquals("0.5", edge.property("weight"));
            }
        }
        Assert.assertTrue(interestedEdge);
    }

    @Test
    public void testCustomizedSchema() {
        FileUtil.append(storePath("vertex_person.csv"),
                        "name,age,city",
                        "marko,29,Beijing",
                        "vadas,27,Hongkong",
                        "josh,32,Beijing",
                        "peter,35,Shanghai",
                        "\"li,nary\",26,\"Wu,han\"");

        FileUtil.append(storePath("vertex_software.csv"), GBK,
                        "name,lang,price",
                        "lop,java,328",
                        "ripple,java,199");

        FileUtil.append(storePath("edge_knows.csv"),
                        "source_name,target_name,date,weight",
                        "marko,vadas,20160110,0.5",
                        "marko,josh,20130220,1.0");

        FileUtil.append(storePath("edge_created.csv"),
                        "source_name,target_name,date,weight",
                        "marko,lop,20171210,0.4",
                        "josh,lop,20091111,0.4",
                        "josh,ripple,20171210,1.0",
                        "peter,lop,20170324,0.2");

        String[] args = new String[]{
                "-f", configPath("customized_schema/struct.json"),
                "-s", configPath("customized_schema/schema.groovy"),
                "-g", GRAPH,
                "--num-threads", "2",
                "--test-mode", "true"
        };
        HugeGraphLoader.main(args);

        List<Vertex> vertices = CLIENT.graph().listVertices();
        List<Edge> edges = CLIENT.graph().listEdges();

        Assert.assertEquals(7, vertices.size());
        Assert.assertEquals(6, edges.size());

        boolean interestedVertex = false;
        for (Vertex vertex : vertices) {
            Assert.assertEquals(String.class, vertex.id().getClass());
            if (((String) vertex.id()).contains("li,nary")) {
                interestedVertex = true;
                Assert.assertEquals(26, vertex.property("age"));
                Assert.assertEquals("Wu,han", vertex.property("city"));
            }
        }
        Assert.assertTrue(interestedVertex);

        boolean interestedEdge = false;
        for (Edge edge : edges) {
            Assert.assertEquals(String.class, edge.source().getClass());
            Assert.assertEquals(String.class, edge.target().getClass());
            if (((String) edge.source()).contains("marko") &&
                ((String) edge.target()).contains("vadas")) {
                interestedEdge = true;
                Assert.assertEquals("20160110", edge.property("date"));
                Assert.assertEquals(0.5, edge.property("weight"));
            }
        }
        Assert.assertTrue(interestedEdge);
    }

    @Test
    public void testVertexIdExceedLimit() {
        Integer[] array = new Integer[129];
        Arrays.fill(array, 1);
        String tooLongId = StringUtils.join(array);
        String line = FileUtil.newCSVLine(tooLongId, 29, "Beijing");
        FileUtil.append(storePath("vertex_person.csv"),
                        "name,age,city",
                        line);

        String[] args = new String[]{
                "-f", configPath("vertex_id_exceed_limit/struct.json"),
                "-s", configPath("vertex_id_exceed_limit/schema.groovy"),
                "-g", GRAPH,
                "--num-threads", "2",
                "--test-mode", "true"
        };

        Assert.assertThrows(ParseException.class, () -> {
            HugeGraphLoader.main(args);
        });
    }

    @Test
    public void testVertexIdExceedLimitInBytes() {
        String pk = "ecommerce__color__极光银翻盖上盖+" +
                    "琥珀啡翻盖下盖+咖啡金翻盖上盖装饰片+" +
                    "香槟金主镜片+深咖啡色副镜片+琥珀>" +
                    "啡前壳+极光银后壳+浅灰电池扣+极光银电池组件+深灰天线";
        Assert.assertTrue(pk.length() < 128);
        String line = FileUtil.newCSVLine(pk, "中文", 328);
        FileUtil.append(storePath("vertex_software.csv"), GBK,
                        "name,lang,price",
                        line);

        String[] args = new String[]{
                "-f", configPath("vertex_id_exceed_limit_in_bytes/struct.json"),
                "-s", configPath("vertex_id_exceed_limit_in_bytes/schema.groovy"),
                "-g", GRAPH,
                "--num-threads", "2",
                "--test-mode", "true"
        };
        // Bytes encoded in utf-8 exceed 128
        Assert.assertThrows(ParseException.class, () -> {
            HugeGraphLoader.main(args);
        });
    }

    @Test
    public void testTooManyColumns() {
        FileUtil.append(storePath("vertex_person.csv"),
                        "name,age,city",
                        "marko,29,Beijing,Extra");

        String[] args = new String[]{
                "-f", configPath("too_many_columns/struct.json"),
                "-s", configPath("too_many_columns/schema.groovy"),
                "-g", GRAPH,
                "--num-threads", "2",
                "--test-mode", "true"
        };

        Assert.assertThrows(ParseException.class, () -> {
            HugeGraphLoader.main(args);
        });
    }

    @Test
    public void testTooFewColumns() {
        FileUtil.append(storePath("vertex_person.csv"),
                        "name,age,city",
                        "marko,29");

        String[] args = new String[]{
                "-f", configPath("too_few_columns/struct.json"),
                "-s", configPath("too_few_columns/schema.groovy"),
                "-g", GRAPH,
                "--num-threads", "2",
                "--test-mode", "true"
        };

        Assert.assertThrows(ParseException.class, () -> {
            HugeGraphLoader.main(args);
        });
    }

    @Test
    public void testUnmatchedPropertyDataType() {
        FileUtil.append(storePath("vertex_person.csv"),
                        "name,age,city",
                        "marko,Should be number,Beijing");

        String[] args = new String[]{
                "-f", configPath("unmatched_property_datatype/struct.json"),
                "-s", configPath("unmatched_property_datatype/schema.groovy"),
                "-g", GRAPH,
                "--num-threads", "2",
                "--test-mode", "true"
        };

        Assert.assertThrows(ParseException.class, () -> {
            HugeGraphLoader.main(args);
        });
    }

    @Test
    public void testVertexPkContainsSpecicalSymbol() {
        FileUtil.append(storePath("vertex_person.csv"),
                        "name,age,city",
                        "mar:ko!,29,Beijing");

        String[] args = new String[]{
                "-f", configPath("vertex_pk_contains_special_symbol/struct.json"),
                "-s", configPath("vertex_pk_contains_special_symbol/schema.groovy"),
                "-g", GRAPH,
                "--num-threads", "2",
                "--test-mode", "true"
        };

        HugeGraphLoader.main(args);

        List<Vertex> vertices = CLIENT.graph().listVertices();
        Assert.assertEquals(1, vertices.size());
        Vertex vertex = vertices.get(0);
        Assert.assertEquals(String.class, vertex.id().getClass());
        Assert.assertTrue(((String) vertex.id()).contains(":mar`:ko`!"));
        Assert.assertEquals(29, vertex.property("age"));
        Assert.assertEquals("Beijing", vertex.property("city"));
    }

    @Test
    public void testUnmatchedEncodingCharset() {
        FileUtil.append(storePath("vertex_software.csv"), GBK,
                        "name,lang,price",
                        "lop,中文,328");

        String[] args = new String[]{
                "-f", configPath("unmatched_encoding_charset/struct.json"),
                "-g", GRAPH,
                "-s", configPath("unmatched_encoding_charset/schema.groovy"),
                "--num-threads", "2",
                "--test-mode", "true"
        };
        HugeGraphLoader.main(args);

        List<Vertex> vertices = CLIENT.graph().listVertices();
        Assert.assertEquals(1, vertices.size());
        Vertex vertex = vertices.get(0);
        Assert.assertEquals("lop", vertex.property("name"));
        Assert.assertNotEquals("中文", vertex.property("lang"));
        Assert.assertEquals(328.0, vertex.property("price"));
    }

    @Test
    public void testMatchedEncodingCharset() {
        FileUtil.append(storePath("vertex_software.csv"), GBK,
                        "name,lang,price",
                        "lop,中文,328");

        String[] args = new String[]{
                "-f", configPath("matched_encoding_charset/struct.json"),
                "-g", GRAPH,
                "-s", configPath("matched_encoding_charset/schema.groovy"),
                "--num-threads", "2",
                "--test-mode", "true"
        };
        HugeGraphLoader.main(args);

        List<Vertex> vertices = CLIENT.graph().listVertices();
        Assert.assertEquals(1, vertices.size());
        Vertex vertex = vertices.get(0);
        Assert.assertEquals("lop", vertex.property("name"));
        Assert.assertEquals("中文", vertex.property("lang"));
        Assert.assertEquals(328.0, vertex.property("price"));
    }

    @Test
    public void testValueListPropertyInJsonFile() {
        FileUtil.append(storePath("vertex_person.csv"),
                        "name,age,city",
                        "marko,29,Beijing");
        FileUtil.append(storePath("vertex_software.csv"), GBK,
                        "name,lang,price",
                        "lop,中文,328");
        FileUtil.append(storePath("edge_use.json"),
                        "{\"person_name\": \"marko\", \"software_name\": " +
                        "\"lop\", \"feel\": [\"so so\", \"good\", \"good\"]}");

        String[] args = new String[]{
                "-f", configPath("value_list_property_in_json_file/struct.json"),
                "-g", GRAPH,
                "-s", configPath("value_list_property_in_json_file/schema.groovy"),
                "--num-threads", "2",
                "--test-mode", "true"
        };
        try {
            HugeGraphLoader.main(args);
        } finally {
            FileUtil.delete(storePath("edge_use.json"));
        }

        List<Edge> edges = CLIENT.graph().listEdges();
        Assert.assertEquals(1, edges.size());
        Edge edge = edges.get(0);

        Assert.assertEquals("person", edge.sourceLabel());
        Assert.assertEquals("software", edge.targetLabel());
        Assert.assertEquals(ImmutableList.of("so so", "good", "good"),
                            edge.property("feel"));
    }

    @Test
    public void testValueSetPorpertyInJsonFile() {
        FileUtil.append(storePath("vertex_person.csv"),
                        "name,age,city",
                        "marko,29,Beijing");
        FileUtil.append(storePath("vertex_software.csv"), GBK,
                        "name,lang,price",
                        "lop,中文,328");
        FileUtil.append(storePath("edge_use.json"),
                        "{\"person_name\": \"marko\", \"software_name\": " +
                        "\"lop\", \"time\": [\"20171210\", \"20180101\"]}");

        String[] args = new String[]{
                "-f", configPath("value_set_property_in_json_file/struct.json"),
                "-g", GRAPH,
                "-s", configPath("value_set_property_in_json_file/schema.groovy"),
                "--num-threads", "2",
                "--test-mode", "true"
        };
        try {
            HugeGraphLoader.main(args);
        } finally {
            FileUtil.delete(storePath("edge_use.json"));
        }

        List<Edge> edges = CLIENT.graph().listEdges();
        Assert.assertEquals(1, edges.size());
        Edge edge = edges.get(0);

        Assert.assertEquals("person", edge.sourceLabel());
        Assert.assertEquals("software", edge.targetLabel());
        /*
         * NOTE: Although the cardinality of the property is set in schema
         * declaration, client will deserialize it to list type in default.
         */
        Assert.assertEquals(ImmutableList.of("20171210", "20180101"),
                            edge.property("time"));
    }

    @Test
    public void testCustomizedNumberId() {
        FileUtil.append(storePath("vertex_person_number_id.csv"),
                        "1,marko,29,Beijing",
                        "2,vadas,27,Hongkong");
        FileUtil.append(storePath("edge_knows.csv"), "1,2,20160110,0.5");

        String[] args = new String[]{
                "-f", configPath("customized_number_id/struct.json"),
                "-g", GRAPH,
                "-s", configPath("customized_number_id/schema.groovy"),
                "--test-mode", "true"
        };
        try {
            HugeGraphLoader.main(args);
        } finally {
            FileUtil.delete(storePath("vertex_person_number_id.csv"));
            FileUtil.delete(storePath("edge_knows.csv"));
        }

        List<Vertex> vertices = CLIENT.graph().listVertices();
        Assert.assertEquals(2, vertices.size());

        List<Edge> edges = CLIENT.graph().listEdges();
        Assert.assertEquals(1, edges.size());
    }

    @Test
    public void testVertexJointPrimaryKeys() {
        FileUtil.append(storePath("vertex_person.csv"),
                        "name,age,city",
                        "marko,29,Beijing");

        String[] args = new String[]{
                "-f", configPath("vertex_joint_pks/struct.json"),
                "-s", configPath("vertex_joint_pks/schema.groovy"),
                "-g", GRAPH,
                "--test-mode", "true"
        };
        HugeGraphLoader.main(args);

        List<Vertex> vertices = CLIENT.graph().listVertices();

        Assert.assertEquals(1, vertices.size());
        Vertex vertex = vertices.get(0);

        Assert.assertTrue(vertex.id().toString().contains("marko!Beijing"));
        Assert.assertEquals("person", vertex.label());
        Assert.assertEquals("marko", vertex.property("name"));
        Assert.assertEquals(29, vertex.property("age"));
        Assert.assertEquals("Beijing", vertex.property("city"));
    }

    @Test
    public void testIgnoreLastRedundantEmptyColumn() {
        // Has a redundant seperator at the end of line
        FileUtil.append(storePath("vertex_person.csv"),
                        "name,age,city",
                        "marko,29,Beijing,");

        String[] args = new String[]{
                "-f", configPath("ignore_last_redudant_empty_column/struct.json"),
                "-s", configPath("ignore_last_redudant_empty_column/schema.groovy"),
                "-g", GRAPH,
                "--test-mode", "true"
        };
        HugeGraphLoader.main(args);

        List<Vertex> vertices = CLIENT.graph().listVertices();

        Assert.assertEquals(1, vertices.size());
        Vertex vertex = vertices.get(0);
        Assert.assertEquals(3, vertex.properties().size());
    }

    @Test
    public void testIgnoreNullValueColumns() {
        FileUtil.append(storePath("vertex_person.csv"),
                        "name,age,city",
                        "marko,NULL,null",
                        "vadas,NULL,",
                        "josh,,null");

        String[] args = new String[]{
                "-f", configPath("ignore_null_value_columns/struct.json"),
                "-s", configPath("ignore_null_value_columns/schema.groovy"),
                "-g", GRAPH,
                "--test-mode", "true"
        };
        HugeGraphLoader.main(args);

        List<Vertex> vertices = CLIENT.graph().listVertices();
        Assert.assertEquals(3, vertices.size());

        for (Vertex vertex : vertices) {
            Assert.assertNull(vertex.property("age"));
            Assert.assertNull(vertex.property("city"));
        }
    }

    @Test
    public void testFileOnlyHasAnEmptyLine() {
        FileUtil.append(storePath("vertex_person_empty.csv"), "");

        String[] args = new String[]{
                "-f", configPath("file_only_has_empty_line/struct.json"),
                "-s", configPath("file_only_has_empty_line/schema.groovy"),
                "-g", GRAPH,
                "--test-mode", "true"
        };
        Assert.assertThrows(LoadException.class, () -> {
            HugeGraphLoader.main(args);
        });
        FileUtil.delete(storePath("vertex_person_empty.csv"));
    }

    @Test
    public void testMultiFilesHaveHeader() {
        FileUtil.append(storePath("vertex_dir/vertex_person_1.csv"),
                        "name,age,city",
                        "marko,29,Beijing");
        FileUtil.append(storePath("vertex_dir/vertex_person_2.csv"),
                        "name,age,city",
                        "vadas,27,Hongkong");

        String[] args = new String[]{
                "-f", configPath("multi_files_have_header/struct.json"),
                "-s", configPath("multi_files_have_header/schema.groovy"),
                "-g", GRAPH,
                "--test-mode", "true"
        };

        try {
            HugeGraphLoader.main(args);
        } finally {
            FileUtil.delete(storePath("vertex_dir"));
        }

        List<Vertex> vertices = CLIENT.graph().listVertices();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testFileHasCommentLine() {
        FileUtil.append(storePath("vertex_person.csv"),
                        "name,age,city",
                        "# This is a comment",
                        "marko,29,Beijing",
                        "// This is also a comment",
                        "# This is still a comment",
                        "vadas,27,Hongkong");

        String[] args = new String[]{
                "-f", configPath("file_has_comment_line/struct.json"),
                "-s", configPath("file_has_comment_line/schema.groovy"),
                "-g", GRAPH,
                "--test-mode", "true"
        };
        HugeGraphLoader.main(args);

        List<Vertex> vertices = CLIENT.graph().listVertices();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testDirHasNoFile() {
        FileUtil.mkdirs(storePath("vertex_dir"));
        String[] args = new String[]{
                "-f", configPath("dir_has_no_file/struct.json"),
                "-s", configPath("dir_has_no_file/schema.groovy"),
                "-g", GRAPH,
                "--test-mode", "true"
        };
        try {
            HugeGraphLoader.main(args);
        } finally {
            FileUtil.delete(storePath("vertex_dir"));
        }

        List<Vertex> vertices = CLIENT.graph().listVertices();
        Assert.assertEquals(0, vertices.size());
    }

    @Test
    public void testDirHasMultiFiles() {
        FileUtil.append(storePath("vertex_dir/vertex_person1.csv"),
                        "marko,29,Beijing",
                        "vadas,27,Hongkong",
                        "josh,32,Beijing");
        FileUtil.append(storePath("vertex_dir/vertex_person2.csv"),
                        "peter,35,Shanghai",
                        "\"li,nary\",26,\"Wu,han\"");
        FileUtil.append(storePath("vertex_dir/vertex_person3.csv"));

        String[] args = new String[]{
                "-f", configPath("dir_has_multi_files/struct.json"),
                "-s", configPath("dir_has_multi_files/schema.groovy"),
                "-g", GRAPH,
                "--test-mode", "true"
        };
        try {
            HugeGraphLoader.main(args);
        } finally {
            FileUtil.delete(storePath("vertex_dir"));
        }

        List<Vertex> vertices = CLIENT.graph().listVertices();
        Assert.assertEquals(5, vertices.size());
    }

    @Test
    public void testMatchedDatePropertyAndFormat() {
        FileUtil.append(storePath("vertex_person_birth_date.csv"),
                        "marko,1992-10-01,Beijing",
                        "vadas,2000-01-01,Hongkong");

        // DateFormat is yyyy-MM-dd
        String[] args = new String[]{
                "-f", configPath("matched_date_property_format/struct.json"),
                "-s", configPath("matched_date_property_format/schema.groovy"),
                "-g", GRAPH,
                "--test-mode", "true"
        };
        try {
            HugeGraphLoader.main(args);
        } finally {
            FileUtil.delete(storePath("vertex_person_birth_date.csv"));
        }

        List<Vertex> vertices = CLIENT.graph().listVertices();
        Assert.assertEquals(2, vertices.size());
    }

    @Test
    public void testUnMatchedDatePropertyAndFormat() {
        FileUtil.append(storePath("vertex_person_birth_date.csv"),
                        "marko,1992/10/01,Beijing",
                        "vadas,2000/01/01,Hongkong");

        // DateFormat is yyyy-MM-dd
        String[] args = new String[]{
                "-f", configPath("unmatched_date_property_format/struct.json"),
                "-s", configPath("unmatched_date_property_format/schema.groovy"),
                "-g", GRAPH,
                "--test-mode", "true"
        };
        Assert.assertThrows(ParseException.class, () -> {
            HugeGraphLoader.main(args);
        });

        FileUtil.delete(storePath("vertex_person_birth_date.csv"));
    }
}
