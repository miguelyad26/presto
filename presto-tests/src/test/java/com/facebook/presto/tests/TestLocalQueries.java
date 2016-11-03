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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static org.testng.Assert.assertEquals;

public class TestLocalQueries
        extends AbstractTestQueries
{
    public TestLocalQueries()
    {
        super(createLocalQueryRunner());
    }

    private static LocalQueryRunner createLocalQueryRunner()
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("local")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        LocalQueryRunner localQueryRunner = new LocalQueryRunner(defaultSession);

        // add the tpch catalog
        // local queries run directly against the generator
        localQueryRunner.createCatalog(
                defaultSession.getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.<String, String>of());

        localQueryRunner.getMetadata().addFunctions(CUSTOM_FUNCTIONS);

        SessionPropertyManager sessionPropertyManager = localQueryRunner.getMetadata().getSessionPropertyManager();
        sessionPropertyManager.addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        sessionPropertyManager.addConnectorSessionProperties(new ConnectorId(TESTING_CATALOG), TEST_CATALOG_PROPERTIES);

        return localQueryRunner;
    }

    @Test
    public void testExplainPlanGivesConsistentOutputInSubsequentRuns()
    {
        //q9
        String sql = "SELECT\n" +
                "  nation,\n" +
                "  o_year,\n" +
                "  sum(amount) AS sum_profit\n" +
                "FROM (\n" +
                "       SELECT\n" +
                "         n.name                                                          AS nation,\n" +
                "         extract(YEAR FROM o.orderdate)                                  AS o_year,\n" +
                "         l.extendedprice * (1 - l.discount) - ps.supplycost * l.quantity AS amount\n" +
                "       FROM\n" +
                "         part p,\n" +
                "         supplier s,\n" +
                "         lineitem l,\n" +
                "         partsupp ps,\n" +
                "         orders o,\n" +
                "         nation n\n" +
                "       WHERE\n" +
                "         s.suppkey = l.suppkey\n" +
                "         AND ps.suppkey = l.suppkey\n" +
                "         AND ps.partkey = l.partkey\n" +
                "         AND p.partkey = l.partkey\n" +
                "         AND o.orderkey = l.orderkey\n" +
                "         AND s.nationkey = n.nationkey\n" +
                "         AND p.name LIKE '%green%'\n" +
                "     ) AS profit\n" +
                "GROUP BY\n" +
                "  nation,\n" +
                "  o_year\n" +
                "ORDER BY\n" +
                "  nation,\n" +
                "  o_year DESC";

        MaterializedResult result = computeActual("EXPLAIN " + sql);

        System.out.println("================================================");

        MaterializedResult result2 = computeActual("EXPLAIN " + sql);

        int i = 0;
        while (i < 10) {
            i++;
            System.out.println(result2.toString());

            assertEquals(moduloSymbolNumbers(result2), moduloSymbolNumbers(result), "Explain differs after " + i + " attempts");
            result = result2;
            System.out.println("================================================");
            result2 = computeActual("EXPLAIN " + sql);
        }
    }

    private String moduloSymbolNumbers(MaterializedResult result2)
    {
        return result2.toString().replaceAll("hashvalue_\\d+", "hashvalue_");
    }
}
