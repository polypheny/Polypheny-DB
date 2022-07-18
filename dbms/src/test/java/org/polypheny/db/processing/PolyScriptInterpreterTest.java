/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.processing;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.polypheny.db.PolyResult;
import org.polypheny.db.languages.polyscript.Expression;
import org.polypheny.db.languages.polyscript.Script;
import org.polypheny.db.languages.polyscript.SqlExpression;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;

import java.util.List;
import java.util.Map;


class PolyScriptInterpreterTest {

    @Test
    void interprete() {
        SqlProcessorFacade sqlProcessorFacade = Mockito.mock(SqlProcessorFacade.class);

        Transaction transactionManager = Mockito.mock(Transaction.class);
        PolyScriptInterpreter sut = new PolyScriptInterpreter(sqlProcessorFacade, transactionManager);

        PolyResult result = sut.interprete("sql(select * from emps);", Map.of());
    }

    @Test
    void interpreteParameterized() {
        SqlProcessorFacade sqlProcessorFacade = Mockito.mock(SqlProcessorFacade.class);
        Transaction transactionManager = Mockito.mock(Transaction.class);
        PolyScriptInterpreter sut = new PolyScriptInterpreter(sqlProcessorFacade, transactionManager);

        String query = "sql(insert into students VALUES(:id, :name));";
        Map<String, Object> arguments = Map.of("id", 1, "name", "James");
        PolyResult result = sut.interprete(query, arguments);
    }

    @Test
    void interpreteParameterized1() {
        SqlProcessorFacade sqlProcessorFacade = Mockito.mock(SqlProcessorFacade.class);
        Transaction transactionManager = Mockito.mock(Transaction.class);
        PolyScriptInterpreter sut = new PolyScriptInterpreter(sqlProcessorFacade, transactionManager);

        String query = "sql(insert into customers VALUES('John'));";
        Map<String, Object> arguments = Map.of("id", 1, "name", "James");
        PolyResult result = sut.interprete(query, arguments);
    }
    @Test
    void interpreteParameterized2() {
        SqlProcessorFacade sqlProcessorFacade = Mockito.mock(SqlProcessorFacade.class);
        Transaction transactionManager = Mockito.mock(Transaction.class);
        PolyScriptInterpreter sut = new PolyScriptInterpreter(sqlProcessorFacade, transactionManager);

        String query = "sql(insert into customers VALUES(:id, :name));";
        Map<String, Object> arguments = Map.of("id", 1, "name", "James");
        PolyResult result = sut.interprete(query, arguments);
    }
}