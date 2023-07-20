/*
 * Copyright 2019-2023 The Polypheny Project
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

import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.DeadlockException;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class ProcessorTest {

    @Mock
    private Processor processor;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    @DisplayName("Should throw a RuntimeException when a TransactionException is caught")
    void prepareDdlWhenTransactionExceptionIsCaughtThenThrowException() throws TransactionException {
        Statement statement = mock(Statement.class);
        ExecutableStatement parsed = mock(ExecutableStatement.class);
        QueryParameters parameters = new QueryParameters("CREATE TABLE students (id INT, name VARCHAR)", NamespaceType.RELATIONAL);
        when(processor.getResult(statement, parsed, parameters)).thenThrow(new TransactionException("Transaction exception"));

        // Act and Assert
        assertThrows(RuntimeException.class, () -> processor.prepareDdl(statement, parsed, parameters));
    }

    @Test
    @DisplayName("Should throw a RuntimeException when a DeadlockException is caught")
    void prepareDdlWhenDeadlockExceptionIsCaughtThenThrowException() throws TransactionException {
        Statement statement = mock(Statement.class);
        ExecutableStatement parsed = mock(ExecutableStatement.class);
        QueryParameters parameters = new QueryParameters("CREATE TABLE students (id INT, name VARCHAR)", NamespaceType.RELATIONAL);
        when(processor.getResult(statement, parsed, parameters)).thenThrow(new DeadlockException(new Exception()));

        // Act and Assert
        assertThrows(RuntimeException.class, () -> processor.prepareDdl(statement, parsed, parameters));
    }

    @Test
    @DisplayName("Should return the result when the parsed node is an instance of ExecutableStatement")
    void prepareDdlWhenParsedNodeIsExecutableStatement() throws TransactionException {
        Statement statement = mock(Statement.class);
        ExecutableStatement parsed = mock(ExecutableStatement.class);
        QueryParameters parameters = new QueryParameters("CREATE TABLE students (id INT, name VARCHAR)", NamespaceType.RELATIONAL);
        PolyImplementation expected = mock(PolyImplementation.class);

        when(processor.getResult(statement, parsed, parameters)).thenReturn(expected);

        PolyImplementation result = processor.prepareDdl(statement, parsed, parameters);

        assertEquals(expected, result);
        verify(processor, times(1)).getResult(statement, parsed, parameters);
    }

    @Test
    @DisplayName("Should throw a RuntimeException when the parsed node is not an instance of ExecutableStatement")
    void prepareDdlWhenParsedNodeIsNotExecutableStatementThenThrowException() {
        Statement statement = mock(Statement.class);
        Node parsed = mock(Node.class);
        QueryParameters parameters = new QueryParameters("CREATE TABLE students (id INT, name VARCHAR)", NamespaceType.RELATIONAL);

        // Act and Assert
        assertThrows(RuntimeException.class, () -> {
            processor.prepareDdl(statement, parsed, parameters);
        });

        // Verify
        verify(statement, never()).getPrepareContext();
        verify(statement, never()).getTransaction();
        verify(statement, never()).commit();
        verifyNoMoreInteractions(statement);
    }

}