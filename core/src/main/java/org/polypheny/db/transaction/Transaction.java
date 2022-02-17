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

package org.polypheny.db.transaction;


import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.processing.DataMigrator;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.schema.PolyphenyDbSchema;


public interface Transaction {

    long getId();

    PolyXid getXid();

    Statement createStatement();

    void commit() throws TransactionException;

    void rollback() throws TransactionException;

    void registerInvolvedAdapter( Adapter adapter );

    List<Adapter> getInvolvedAdapters();

    PolyphenyDbSchema getSchema();

    boolean isActive();

    JavaTypeFactory getTypeFactory();

    PolyphenyDbCatalogReader getCatalogReader();

    Processor getProcessor( QueryLanguage language );

    boolean isAnalyze();

    void setAnalyze( boolean analyze );

    InformationManager getQueryAnalyzer();

    AtomicBoolean getCancelFlag();

    CatalogSchema getDefaultSchema();

    void addChangedTable( String qualifiedTableName );

    String getOrigin();

    MultimediaFlavor getFlavor();

    long getNumberOfStatements();

    DataMigrator getDataMigrator();

    void setUseCache( boolean useCache );

    boolean getUseCache();

    Set<CatalogTable> getCatalogTables();

    /**
     * Flavor, how multimedia results should be returned from a store.
     */
    enum MultimediaFlavor {
        DEFAULT, FILE
    }

}
