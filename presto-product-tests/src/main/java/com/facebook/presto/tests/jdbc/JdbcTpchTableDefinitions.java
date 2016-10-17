/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.tests.jdbc;

import com.google.common.collect.ImmutableList;
import com.teradata.tempto.fulfillment.table.hive.tpch.TpchTable;
import com.teradata.tempto.fulfillment.table.jdbc.JdbcTableDefinition;

import java.sql.JDBCType;

import static java.sql.JDBCType.BIGINT;
import static java.sql.JDBCType.VARCHAR;

public final class JdbcTpchTableDefinitions
{
    private JdbcTpchTableDefinitions() {}

    public static final ImmutableList<JDBCType> NATION_TYPES = ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR);

    public static final JdbcTableDefinition JDBC_NATION =
            JdbcTableDefinition.builder("nation")
                    .setCreateTableDDLTemplate("CREATE TABLE %NAME%(" +
                            "   n_nationkey     BIGINT," +
                            "   n_name          VARCHAR," +
                            "   n_regionkey     BIGINT," +
                            "   n_comment       VARCHAR)")
                    .setDataSource(new JdbcTpchDataSource(TpchTable.NATION, NATION_TYPES, 1.0))
                    .build();
}
