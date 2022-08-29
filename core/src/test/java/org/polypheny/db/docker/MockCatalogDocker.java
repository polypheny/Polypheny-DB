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

package org.polypheny.db.docker;


import java.util.*;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Event;
import org.polypheny.db.catalog.MockCatalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogProcedure;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogTrigger;
import org.polypheny.db.catalog.exceptions.ProcedureAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownProcedureException;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.Pair;

/**
 * This is a bare-bone catalog which allows to mock register adapters
 * which then can be retrieved while testing adapters which use Docker
 */
public class MockCatalogDocker extends MockCatalog {

    int i = 0;
    HashMap<Integer, CatalogAdapter> adapters = new HashMap<>();


    @Override
    public int addAdapter( String uniqueName, String clazz, AdapterType type, Map<String, String> settings ) {
        i++;
        adapters.put( i, new CatalogAdapter( i, uniqueName, clazz, type, settings ) );
        return i;
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
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogTrigger> getTriggers(String schemaName, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Long, AlgNode> getTriggerNodes() {
        return null;
    }

    @Override
    public Optional<CatalogProcedure> getProcedure(long databaseId, long schemaId, String tableName) throws UnknownProcedureException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<CatalogProcedure> getProcedure(Object[] key) throws UnknownProcedureException {
        return Optional.empty();
    }

    @Override
    public void deleteProcedure(long databaseId, long schemaId, String procedureName) throws UnknownProcedureException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTrigger(long databaseId, long schemaId, String triggerName, CatalogTable table, Event event, String query, AlgNode algNode, QueryLanguage language) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTrigger(long databaseId, Long schemaId, String triggerName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean checkIfExistsAdapter( int adapterId ) {
        return adapters.containsKey( adapterId );
    }

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
    public CatalogAdapter getAdapter( int adapterId ) {
        return adapters.get( adapterId );
    }

}
