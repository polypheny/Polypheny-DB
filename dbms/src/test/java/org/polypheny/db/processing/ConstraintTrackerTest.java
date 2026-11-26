/*
 * Copyright 2019-2026 The Polypheny Project
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.ConfigBoolean;
import org.polypheny.db.processing.ConstraintEnforceAttacher.ConstraintTracker;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.transaction.locking.LockablesRegistry;

class ConstraintTrackerTest {

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void failedEnablePreservesTheCurrentValue( boolean previous ) {
        TransactionManager manager = mock( TransactionManager.class );
        ConfigBoolean flag = trackedFlag( manager, previous );
        ConfigListener observer = mock( ConfigListener.class );
        flag.addObserver( observer );
        IllegalStateException failure = new IllegalStateException( "validation failed" );
        when( manager.startTransaction( Catalog.defaultUserId, null, "ConstraintEnforcement" ) ).thenThrow( failure );

        assertSame( failure, assertThrows( IllegalStateException.class, () -> flag.setBoolean( true ) ) );

        assertEquals( previous, flag.getBoolean() );
        verify( manager ).startTransaction( Catalog.defaultUserId, null, "ConstraintEnforcement" );
        verifyNoInteractions( observer );
    }


    @Test
    void failedAlterConfigKeepsTheValidationErrorAndLeavesTheTransactionToItsCaller() {
        TransactionManager manager = mock( TransactionManager.class );
        ConfigBoolean flag = trackedFlag( manager, false );
        ConfigListener observer = mock( ConfigListener.class );
        flag.addObserver( observer );
        Transaction borrowed = mock( Transaction.class );
        TransactionException violation = new TransactionException( "constraint violated", "23505" );
        when( borrowed.getSnapshot() ).thenThrow( violation );

        GenericRuntimeException failure = assertThrows( GenericRuntimeException.class,
                () -> ConstraintTracker.setConfigValue( flag, "true", borrowed ) );

        assertEquals( "Could not change the constraints. \nconstraint violated", failure.getMessage() );
        assertSame( violation, failure.getCause() );
        assertEquals( "23505", violation.getSqlState() );
        assertFalse( flag.getBoolean() );
        verify( borrowed ).getSnapshot();
        verifyNoMoreInteractions( borrowed );
        verifyNoInteractions( manager, observer );

        IllegalStateException nextFailure = new IllegalStateException( "standalone validation failed" );
        when( manager.startTransaction( Catalog.defaultUserId, null, "ConstraintEnforcement" ) ).thenThrow( nextFailure );
        assertSame( nextFailure, assertThrows( IllegalStateException.class, () -> flag.setBoolean( true ) ) );
        verify( manager ).startTransaction( Catalog.defaultUserId, null, "ConstraintEnforcement" );
    }


    @Test
    void successfulEnableValidatesBeforeNotifyingObservers() {
        TransactionManager manager = mock( TransactionManager.class );
        ConfigBoolean flag = trackedFlag( manager, false );
        Transaction transaction = emptyValidationTransaction( manager );
        doAnswer( invocation -> {
            assertFalse( flag.getBoolean() );
            invocation.getArgument( 0, Runnable.class ).run();
            assertTrue( flag.getBoolean() );
            return null;
        } ).when( transaction ).commit( any( Runnable.class ) );

        ConfigListener observer = mock( ConfigListener.class );
        doAnswer( invocation -> {
            assertTrue( flag.getBoolean() );
            verify( transaction ).commit( any( Runnable.class ) );
            return null;
        } ).when( observer ).onConfigChange( flag );
        flag.addObserver( observer );

        assertTrue( flag.setBoolean( true ) );
        assertTrue( flag.getBoolean() );
        InOrder order = inOrder( transaction );
        order.verify( transaction ).acquireLockable( LockablesRegistry.GLOBAL_SCHEMA_LOCKABLE, LockType.EXCLUSIVE );
        order.verify( transaction ).getSnapshot();
        order.verify( transaction ).commit( any( Runnable.class ) );
        verify( observer ).onConfigChange( flag );

        flag.removeObserver( observer );
        assertTrue( flag.setBoolean( false ) );
        assertFalse( flag.getBoolean() );
        verify( manager ).startTransaction( Catalog.defaultUserId, null, "ConstraintEnforcement" );
    }


    @Test
    void failedCommitPreservesTheCurrentValueAndCause() {
        TransactionManager manager = mock( TransactionManager.class );
        ConfigBoolean flag = trackedFlag( manager, false );
        Transaction transaction = emptyValidationTransaction( manager );
        ConfigListener observer = mock( ConfigListener.class );
        flag.addObserver( observer );
        TransactionException commitFailure = new TransactionException( "commit failed", "40001" );
        doThrow( commitFailure ).when( transaction ).commit( any( Runnable.class ) );

        GenericRuntimeException failure = assertThrows( GenericRuntimeException.class, () -> flag.setBoolean( true ) );

        assertEquals( "Error while committing constraint enforcement check, the transaction was rolled back!commit failed", failure.getMessage() );
        assertSame( commitFailure, failure.getCause() );
        assertFalse( flag.getBoolean() );
        verify( transaction ).commit( any( Runnable.class ) );
        verifyNoInteractions( observer );
    }


    private static Transaction emptyValidationTransaction( TransactionManager manager ) {
        Transaction transaction = mock( Transaction.class );
        Snapshot snapshot = mock( Snapshot.class );
        Statement statement = mock( Statement.class );
        when( manager.startTransaction( Catalog.defaultUserId, null, "ConstraintEnforcement" ) ).thenReturn( transaction );
        when( transaction.getSnapshot() ).thenReturn( snapshot );
        when( snapshot.getNamespaces( null ) ).thenReturn( List.of() );
        when( transaction.createStatement() ).thenReturn( statement );
        return transaction;
    }


    private static ConfigBoolean trackedFlag( TransactionManager manager, boolean initialValue ) {
        ConfigBoolean flag = new ConfigBoolean( "test.constraintEnforcement", initialValue );
        ConstraintTracker tracker = new ConstraintTracker( manager );
        flag.setUpdateHandler( tracker.constraintUpdate( () -> true, () -> false ) );
        return flag;
    }

}
