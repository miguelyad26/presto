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

import com.google.common.base.Splitter;
import com.teradata.tempto.fulfillment.table.hive.tpch.TpchTable;
import com.teradata.tempto.fulfillment.table.jdbc.JdbcTableDataSource;
import com.teradata.tempto.internal.query.QueryRowMapper;
import io.airlift.tpch.TpchEntity;

import java.sql.JDBCType;
import java.util.Iterator;
import java.util.List;
import java.util.stream.StreamSupport;

public class JdbcTpchDataSource
        implements JdbcTableDataSource
{
    private static final Splitter SPLITTER = Splitter.on('|');

    private final TpchTable table;
    private final List<JDBCType> columnTypes;
    private final double scaleFactor;

    public JdbcTpchDataSource(TpchTable table, List<JDBCType> columnTypes, double scaleFactor)
    {
        this.table = table;
        this.columnTypes = columnTypes;
        this.scaleFactor = scaleFactor;
    }

    @Override
    public Iterator<List<Object>> getDataRows()
    {
        @SuppressWarnings("unchecked")
        Iterable<? extends io.airlift.tpch.TpchEntity> tableDataGenerator = table.getTpchTableEntity().createGenerator(scaleFactor, 1, 1);
        return StreamSupport.stream(tableDataGenerator.spliterator(), false)
                .map(this::tpchEntityToObjects)
                .iterator();
    }

    private List<Object> tpchEntityToObjects(TpchEntity entity)
    {
        List<String> columnValues = SPLITTER.splitToList(entity.toLine());
        QueryRowMapper queryRowMapper = new QueryRowMapper(columnTypes);
        List<String> valuesWithoutFinalBlank = columnValues.subList(0, columnValues.size() - 1);
        return queryRowMapper.mapToRow(valuesWithoutFinalBlank).getValues();
    }
}
