/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.mql.mql2alg;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Event;
import org.polypheny.db.catalog.MockCatalog;
import org.polypheny.db.catalog.entity.*;
import org.polypheny.db.catalog.exceptions.ProcedureAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownProcedureException;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.Optional;


public class MqlMockCatalog extends MockCatalog {

    @Override
    public Map<Long, AlgNode> getProcedureNodes() {
        return null;
    }

    @Override
    public CatalogProcedure getProcedure(Long id) {
        return null;
    }

    @Override
    public List<AlgNode> restoreProcedures(Transaction transaction) {
        return null;
    }

    @Override
    public List<AlgNode> restoreTriggers(Transaction transaction) {
        return null;
    }

    @Override
    public CatalogSchema getSchema( long schemaId ) {
        return new CatalogSchema( 1, "private", 0, 0, "tester", SchemaType.DOCUMENT );
    }

    @Override
    public CatalogUser getUser( int userId ) {
        return new CatalogUser( 0, "name", "name", 1 );
    }

    @Override
    public void createProcedure(Long schemaId, String procedureName, Long databaseId, AlgNode query, String queryString, List<Pair<String, Object>> arguments) throws ProcedureAlreadyExistsException {

    }

    @Override
    public void updateProcedure(Long schemaId, String procedureName, Long databaseId, AlgNode query, String queryString, List<Pair<String, Object>> arguments) throws ProcedureAlreadyExistsException {

    }

    @Override
    public List<CatalogProcedure> getProcedures() {
        return null;
    }

    @Override
    public List<CatalogTrigger> getTriggers(Long schemaId) {
        return null;
    }

    @Override
    public List<CatalogTrigger> getTriggers() {
        return null;
    }

    @Override
    public List<CatalogTrigger> getTriggers(String schemaName, String tableName) {
        return null;
    }

    @Override
    public Map<Long, AlgNode> getTriggerNodes() {
        return null;
    }

    @Override
    public Optional<CatalogProcedure> getProcedure(long databaseId, long schemaId, String tableName) {
        return Optional.of(null);
    }

    @Override
    public Optional<CatalogProcedure> getProcedure(Object[] key) throws UnknownProcedureException {
        return Optional.empty();
    }

    @Override
    public void deleteProcedure(long databaseId, long schemaId, String procedureName) {

    }

    @Override
    public void createTrigger(long databaseId, long schemaId, String triggerName, Long tableId, Event event, String query, AlgNode algNode, QueryLanguage language) {

    }

    @Override
    public void dropTrigger(long databaseId, Long schemaId, String triggerName) {

    }

    @Override
    public Optional<CatalogTrigger> getTrigger(Object[] key) {
        return Optional.empty();
    }


}
