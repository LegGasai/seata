/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seata.rm.datasource.undo.kingbase;

import org.apache.seata.rm.datasource.sql.struct.Row;
import org.apache.seata.rm.datasource.sql.struct.TableRecords;
import org.apache.seata.rm.datasource.undo.BaseExecutorTest;
import org.apache.seata.rm.datasource.undo.SQLUndoLog;
import org.apache.seata.sqlparser.SQLType;
import org.apache.seata.sqlparser.struct.TableMeta;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class KingbaseUndoInsertExecutorTest extends BaseExecutorTest {

    @Test
    public void buildUndoSQL() {
        KingbaseUndoInsertExecutor executor = upperCase();
        String sql = executor.buildUndoSQL();
        Assertions.assertNotNull(sql);
        Assertions.assertTrue(sql.contains("DELETE"));
        Assertions.assertTrue(sql.contains("ID"));
        Assertions.assertTrue(sql.contains("TABLE_NAME"));
    }

    @Test
    public void getUndoRows() {
        KingbaseUndoInsertExecutor executor = upperCase();
        Assertions.assertEquals(executor.getUndoRows(), executor.getSqlUndoLog().getAfterImage());
    }

    public KingbaseUndoInsertExecutor upperCase() {
        TableMeta tableMeta = Mockito.mock(TableMeta.class);
        Mockito.when(tableMeta.getPrimaryKeyOnlyName()).thenReturn(Arrays.asList(new String[]{"ID"}));
        Mockito.when(tableMeta.getTableName()).thenReturn("TABLE_NAME");

        TableRecords beforeImage = new TableRecords();
        beforeImage.setTableName("TABLE_NAME");
        beforeImage.setTableMeta(tableMeta);
        List<Row> beforeRows = new ArrayList<>();
        Row row0 = new Row();
        addField(row0, "ID", 1, "1");
        addField(row0, "AGE", 1, "1");
        beforeRows.add(row0);
        Row row1 = new Row();
        addField(row1, "ID", 1, "1");
        addField(row1, "AGE", 1, "1");
        beforeRows.add(row1);
        beforeImage.setRows(beforeRows);

        TableRecords afterImage = new TableRecords();
        afterImage.setTableName("TABLE_NAME");
        afterImage.setTableMeta(tableMeta);
        List<Row> afterRows = new ArrayList<>();
        Row row2 = new Row();
        addField(row2, "ID", 1, "1");
        addField(row2, "AGE", 1, "1");
        afterRows.add(row2);
        Row row3 = new Row();
        addField(row3, "ID", 1, "1");
        addField(row3, "AGE", 1, "1");
        afterRows.add(row3);
        afterImage.setRows(afterRows);

        SQLUndoLog sqlUndoLog = new SQLUndoLog();
        sqlUndoLog.setSqlType(SQLType.INSERT);
        sqlUndoLog.setTableMeta(tableMeta);
        sqlUndoLog.setTableName("TABLE_NAME");
        sqlUndoLog.setBeforeImage(beforeImage);
        sqlUndoLog.setAfterImage(afterImage);

        return new KingbaseUndoInsertExecutor(sqlUndoLog);
    }

}
