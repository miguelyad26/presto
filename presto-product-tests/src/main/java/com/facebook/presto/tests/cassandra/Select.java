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
package com.facebook.presto.tests.cassandra;

import com.teradata.tempto.ProductTest;
import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.assertions.QueryAssert;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static com.facebook.presto.tests.TestGroups.CASSANDRA;
import static com.facebook.presto.tests.cassandra.TestConstants.CONNECTOR_NAME;
import static com.facebook.presto.tests.cassandra.TestConstants.KEY_SPACE;
import static com.facebook.presto.tests.utils.QueryExecutors.onPresto;
import static com.teradata.tempto.assertions.QueryAssert.assertThat;

public class Select
        extends ProductTest
        implements RequirementsProvider
{
    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return null;
//        return immutableTable(RelationalTableDefinition.relationalTableDefinition(
//                TableHandle.tableHandle("nation").inDatabase("cassandra").inSchema(KEY_SPACE),
//                "create table %NAME% (nationkey bigint, primary key(nationkey))",
//                () -> ImmutableList.<List<Object>>of(
//                        ImmutableList.of(1),
//                        ImmutableList.of(2)
//                ).iterator()));
    }

    @Test(groups = CASSANDRA)
    public void testNation()
            throws SQLException
    {
        QueryResult queryResult = onPresto()
                .executeQuery(String.format(
                        "SELECT * FROM %s.%s.%s",
                        CONNECTOR_NAME,
                        KEY_SPACE,
                        "nation"));

        assertThat(queryResult).containsOnly(QueryAssert.Row.row(1, 2, 3, 4));
    }
}
