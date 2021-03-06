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

package com.hazelcast.sql;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.sql.impl.SqlRowImpl;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.support.CalciteSqlTestSupport;
import com.hazelcast.sql.support.ModelGenerator;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JoinSqlJdbcTest extends CalciteSqlTestSupport {
    /** Make sure that we fetch several pages. */
    private static final int PERSON_CNT = SqlStatement.DEFAULT_CURSOR_BUFFER_SIZE * 2;

    private static HazelcastInstance client;

    @BeforeClass
    public static void beforeClass() {
        HazelcastInstance member = Hazelcast.newHazelcastInstance();
        client = HazelcastClient.newHazelcastClient();

        ModelGenerator.generatePerson(member, PERSON_CNT);
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testJoinClient() throws Exception {
        String sql = "SELECT p.name, p.deptTitle FROM person p INNER JOIN department d ON p.deptTitle = d.title";

        // Execute normal query.
        SqlResult cursor = executeQuery(client, sql);
        List<SqlRow> rows = getQueryRows(cursor);

        // Execute JDBC query.
        List<SqlRow> jdbcRows = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection("jdbc:hazelcast://127.0.0.1:5701")) {
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        String name = rs.getString(1);
                        String deptTitle = rs.getString(2);

                        HeapRow row = new HeapRow(2);
                        row.set(0, name);
                        row.set(1, deptTitle);

                        SqlRowMetadata rowMetadata = new SqlRowMetadata(Arrays.asList(
                            new SqlColumnMetadata("name", SqlColumnType.VARCHAR),
                            new SqlColumnMetadata("deptTitle", SqlColumnType.VARCHAR)
                        ));

                        jdbcRows.add(new SqlRowImpl(rowMetadata, row));
                    }
                }
            }
        }

        assertEquals(PERSON_CNT, rows.size());

        checkRows(rows, jdbcRows, 2);
    }

    private static void checkRows(List<SqlRow> rows, List<SqlRow> otherRows, int columnCount) {
        assertEquals(rows.size(), otherRows.size());

        for (int i = 0; i < rows.size(); i++) {
            SqlRow row = rows.get(i);
            SqlRow otherRow = otherRows.get(i);

            for (int j = 0; j < columnCount; j++) {
                assertEquals(row.getObject(j), otherRow.getObject(j));
            }
        }
    }
}
