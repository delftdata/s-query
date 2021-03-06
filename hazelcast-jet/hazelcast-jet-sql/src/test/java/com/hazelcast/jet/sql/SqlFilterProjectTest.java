/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql;

import com.hazelcast.jet.sql.impl.connector.test.AllTypesSqlConnector;
import com.hazelcast.jet.sql.impl.connector.test.TestBatchSqlConnector;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.hazelcast.jet.core.TestUtil.createMap;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class SqlFilterProjectTest extends SqlTestSupport {

    private static SqlService sqlService;

    @BeforeClass
    public static void setUpClass() {
        initialize(2, null);
        sqlService = instance().getSql();
    }

    @Test
    public void test_valuesSelect() {
        assertRowsAnyOrder(
                "SELECT * FROM (VALUES ('a'))",
                singletonList(new Row("a"))
        );
    }

    @Test
    public void test_valuesSelectExpression() {
        assertRowsAnyOrder(
                "SELECT * FROM (VALUES (1), (1 + 2), (CAST ('5' AS TINYINT)))",
                asList(new Row((byte) 1), new Row((byte) 3), new Row((byte) 5))
        );
    }

    @Test
    public void test_valuesSelectFilter() {
        assertRowsAnyOrder(
                "SELECT a - b FROM (VALUES (1, 2), (3, 5), (7, 11)) AS t (a, b) WHERE a > 1",
                asList(
                        new Row((byte) -2),
                        new Row((byte) -4)
                )
        );
    }

    @Test
    public void test_valuesSelectFilterExpression() {
        assertRowsAnyOrder(
                "SELECT a - b FROM ("
                        + "VALUES (1, 2), (3, 5), (7, 11)"
                        + ") AS t (a, b) "
                        + "WHERE a + b + 0 + CAST('1' AS TINYINT) > 4",
                asList(
                        new Row((byte) -2),
                        new Row((byte) -4)
                )
        );
    }

    @Test
    public void test_valuesSelectExpressionFilterExpression() {
        assertRowsAnyOrder(
                "SELECT a - b FROM ("
                        + "VALUES (1, 1 + 1), (3, 5), (CAST('7' AS TINYINT), 11)"
                        + ") AS t (a, b) "
                        + "WHERE a + b + 0 + CAST('1' AS TINYINT) > 4",
                asList(
                        new Row((byte) -2),
                        new Row((byte) -4)
                )
        );
    }

    @Test
    public void test_valuesInsert() {
        sqlService.execute(javaSerializableMapDdl("m", Integer.class, Integer.class));

        assertMapEventually(
                "m",
                "SINK INTO m(__key, this) VALUES (1, 1), (2, 2)",
                createMap(1, 1, 2, 2)
        );
    }

    @Test
    public void test_valuesInsertExpression() {
        sqlService.execute(javaSerializableMapDdl("m", Integer.class, Integer.class));

        assertMapEventually(
                "m",
                "SINK INTO m(__key, this) VALUES (CAST(1 AS INTEGER), CAST(1 + 0 AS INTEGER)), (2, 2)",
                createMap(1, 1, 2, 2)
        );
    }

    @Test
    public void test_projectWithoutInputReferences() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT '???????????????' FROM t",
                asList(
                        new Row("???????????????"),
                        new Row("???????????????")
                )
        );
    }

    @Test
    public void test_starProject() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT * FROM t",
                asList(
                        new Row(0),
                        new Row(1)
                )
        );
    }

    @Test
    public void test_starProjectProject() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT * FROM (SELECT * FROM t)",
                asList(
                        new Row(0),
                        new Row(1)
                )
        );
    }

    @Test
    public void test_starProjectFilterProjectFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT * FROM (SELECT * FROM t WHERE 0 = 0) WHERE 1 = 1",
                asList(
                        new Row(0),
                        new Row(1)
                )
        );
    }

    @Test
    public void test_starProjectFilterExpressionProjectFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT * FROM (SELECT * FROM t WHERE 0 = 0) WHERE 2 - 1 = 1",
                asList(
                        new Row(0),
                        new Row(1)
                )
        );
    }

    @Test
    public void test_project() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT v, v FROM t",
                asList(
                        new Row(0, 0),
                        new Row(1, 1)
                )
        );
    }

    @Test
    public void test_projectProject() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v f2 FROM t)",
                asList(
                        new Row(0, 0),
                        new Row(1, 1)
                )
        );
    }

    @Test
    public void test_projectExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT v + 1, v * v FROM t",
                asList(
                        new Row(1L, 0L),
                        new Row(2L, 1L)
                )
        );
    }

    @Test
    public void test_projectFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT v, v FROM t WHERE v = 1 OR v = 2",
                asList(
                        new Row(1, 1),
                        new Row(2, 2)
                )
        );
    }

    @Test
    public void test_projectFilterExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT v, v FROM t WHERE v + v > 1",
                asList(
                        new Row(1, 1),
                        new Row(2, 2)
                )
        );
    }

    @Test
    public void test_projectExpressionFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT v + 1, v + v FROM t WHERE v >= 1",
                asList(
                        new Row(2L, 2L),
                        new Row(3L, 4L)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT v + 1, v + v FROM t WHERE v + v > 1",
                asList(
                        new Row(2L, 2L),
                        new Row(3L, 4L)
                )
        );
    }

    @Test
    public void test_projectProjectExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v + v f2 FROM t)",
                asList(
                        new Row(0L, 0),
                        new Row(2L, 1)
                )
        );
    }

    @Test
    public void test_projectExpressionProject() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v f2 FROM t)",
                asList(
                        new Row(0L),
                        new Row(2L)
                )
        );
    }

    @Test
    public void test_projectExpressionProjectExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 2);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v + v f2 FROM t)",
                asList(
                        new Row(0L),
                        new Row(3L)
                )
        );
    }

    @Test
    public void test_projectProjectFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v f2 FROM t WHERE v >= 1)",
                asList(
                        new Row(1, 1),
                        new Row(2, 2)
                )
        );
    }

    @Test
    public void test_projectProjectFilterExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v f2 FROM t WHERE v + v > 1)",
                asList(
                        new Row(1, 1),
                        new Row(2, 2)
                )
        );
    }

    @Test
    public void test_projectProjectExpressionFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v + v f2 FROM t WHERE v >= 1)",
                asList(
                        new Row(2L, 1),
                        new Row(4L, 2)
                )
        );
    }

    @Test
    public void test_projectProjectExpressionFilterExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v + v f2 FROM t WHERE v + v > 1)",
                asList(
                        new Row(2L, 1),
                        new Row(4L, 2)
                )
        );
    }

    @Test
    public void test_projectExpressionProjectFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v f2 FROM t WHERE v >= 1)",
                asList(
                        new Row(2L),
                        new Row(4L)
                )
        );
    }

    @Test
    public void test_projectExpressionProjectFilterExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v f2 FROM t WHERE v + v > 1)",
                asList(
                        new Row(2L),
                        new Row(4L)
                )
        );
    }

    @Test
    public void test_projectExpressionProjectExpressionFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v + v f2 FROM t WHERE v >= 1)",
                asList(
                        new Row(3L),
                        new Row(6L)
                )
        );
    }

    @Test
    public void test_projectExpressionProjectExpressionFilterExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v + v f2 FROM t WHERE v + v > 1)",
                asList(
                        new Row(3L),
                        new Row(6L)
                )
        );
    }

    @Test
    public void test_projectFilterProject() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v f2 FROM t) WHERE f2 >= 1",
                asList(
                        new Row(1, 1),
                        new Row(2, 2)
                )
        );
    }

    @Test
    public void test_projectFilterExpressionProject() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v f2 FROM t) WHERE f1 + f2 > 1",
                asList(
                        new Row(1, 1),
                        new Row(2, 2)
                )
        );
    }

    @Test
    public void test_projectFilterProjectExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v + v f2 FROM t) WHERE f1 >= 1",
                asList(
                        new Row(2L, 1),
                        new Row(4L, 2)
                )
        );
    }

    @Test
    public void test_projectFilterExpressionProjectExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v + v f2 FROM t) WHERE f1 + f2 > 2",
                asList(
                        new Row(2L, 1),
                        new Row(4L, 2)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterProject() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v f2 FROM t) WHERE f2 >= 1",
                asList(
                        new Row(2L),
                        new Row(4L)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterExpressionProject() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v f2 FROM t) WHERE f1 + f2 > 1",
                asList(
                        new Row(2L),
                        new Row(4L)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterProjectExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v + v f2 FROM t) WHERE f1 >= 1",
                asList(
                        new Row(3L),
                        new Row(6L)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterExpressionProjectExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 3);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v + v f2 FROM t) WHERE f1 + f2 > 1",
                asList(
                        new Row(3L),
                        new Row(6L)
                )
        );
    }

    @Test
    public void test_projectFilterProjectFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 4);

        assertRowsAnyOrder(
                "SELECT f2, f1 FROM (SELECT v f1, v f2 FROM t WHERE v >= 1) WHERE f2 < 3",
                asList(
                        new Row(1, 1),
                        new Row(2, 2)
                )
        );
    }

    @Test
    public void test_projectFilterProjectExpressionFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 4);

        assertRowsAnyOrder(
                "SELECT f1 FROM (SELECT v f1, v + v f2 FROM t WHERE v >= 1) WHERE f2 < 6",
                asList(
                        new Row(1),
                        new Row(2)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterProjectFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 4);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v f2 FROM t WHERE v >= 1) WHERE f2 < 3",
                asList(
                        new Row(2L),
                        new Row(4L)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterProjectExpressionFilter() {
        TestBatchSqlConnector.create(sqlService, "t", 4);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v + v f2 FROM t WHERE v >= 1) WHERE f2 < 6",
                asList(
                        new Row(3L),
                        new Row(6L)
                )
        );
    }

    @Test
    public void test_projectExpressionFilterExpressionProjectExpressionFilterExpression() {
        TestBatchSqlConnector.create(sqlService, "t", 4);

        assertRowsAnyOrder(
                "SELECT f1 + f2 FROM (SELECT v f1, v + v f2 FROM t WHERE v + v > 1) WHERE f1 + f2 < 9",
                asList(
                        new Row(3L),
                        new Row(6L)
                )
        );
    }

    @Test
    public void test_queryMetadata() {
        AllTypesSqlConnector.create(sqlService, "t");

        SqlResult result = sqlService.execute("SELECT * FROM t");

        assertThat(result.updateCount()).isEqualTo(-1);
        assertThat(result.getRowMetadata().getColumnCount()).isEqualTo(14);
        assertThat(result.getRowMetadata().getColumn(0).getName()).isEqualTo("string");
        assertThat(result.getRowMetadata().getColumn(0).getType()).isEqualTo(SqlColumnType.VARCHAR);
        assertThat(result.getRowMetadata().getColumn(1).getName()).isEqualTo("boolean");
        assertThat(result.getRowMetadata().getColumn(1).getType()).isEqualTo(SqlColumnType.BOOLEAN);
        assertThat(result.getRowMetadata().getColumn(2).getName()).isEqualTo("byte");
        assertThat(result.getRowMetadata().getColumn(2).getType()).isEqualTo(SqlColumnType.TINYINT);
        assertThat(result.getRowMetadata().getColumn(3).getName()).isEqualTo("short");
        assertThat(result.getRowMetadata().getColumn(3).getType()).isEqualTo(SqlColumnType.SMALLINT);
        assertThat(result.getRowMetadata().getColumn(4).getName()).isEqualTo("int");
        assertThat(result.getRowMetadata().getColumn(4).getType()).isEqualTo(SqlColumnType.INTEGER);
        assertThat(result.getRowMetadata().getColumn(5).getName()).isEqualTo("long");
        assertThat(result.getRowMetadata().getColumn(5).getType()).isEqualTo(SqlColumnType.BIGINT);
        assertThat(result.getRowMetadata().getColumn(6).getName()).isEqualTo("float");
        assertThat(result.getRowMetadata().getColumn(6).getType()).isEqualTo(SqlColumnType.REAL);
        assertThat(result.getRowMetadata().getColumn(7).getName()).isEqualTo("double");
        assertThat(result.getRowMetadata().getColumn(7).getType()).isEqualTo(SqlColumnType.DOUBLE);
        assertThat(result.getRowMetadata().getColumn(8).getName()).isEqualTo("decimal");
        assertThat(result.getRowMetadata().getColumn(8).getType()).isEqualTo(SqlColumnType.DECIMAL);
        assertThat(result.getRowMetadata().getColumn(9).getName()).isEqualTo("time");
        assertThat(result.getRowMetadata().getColumn(9).getType()).isEqualTo(SqlColumnType.TIME);
        assertThat(result.getRowMetadata().getColumn(10).getName()).isEqualTo("date");
        assertThat(result.getRowMetadata().getColumn(10).getType()).isEqualTo(SqlColumnType.DATE);
        assertThat(result.getRowMetadata().getColumn(11).getName()).isEqualTo("timestamp");
        assertThat(result.getRowMetadata().getColumn(11).getType()).isEqualTo(SqlColumnType.TIMESTAMP);
        assertThat(result.getRowMetadata().getColumn(12).getName()).isEqualTo("timestampTz");
        assertThat(result.getRowMetadata().getColumn(12).getType()).isEqualTo(SqlColumnType.TIMESTAMP_WITH_TIME_ZONE);
        assertThat(result.getRowMetadata().getColumn(13).getName()).isEqualTo("object");
        assertThat(result.getRowMetadata().getColumn(13).getType()).isEqualTo(SqlColumnType.OBJECT);
    }

    @Test
    public void test_sinkMetadata() {
        sqlService.execute(javaSerializableMapDdl("m", Integer.class, Integer.class));

        SqlResult result = sqlService.execute("SINK INTO m(__key, this) VALUES (1, 1), (2, 2)");

        assertThat(result.updateCount()).isEqualTo(0);
    }
}
