/*
 * Copyright 2019-2024 The Polypheny Project
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

import java.beans.PropertyChangeListener;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.AbstractAdapterSetting;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager.Function4;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.catalog.MockCatalog;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.iface.QueryInterfaceManager.QueryInterfaceTemplate;


public class MqlMockCatalog extends MockCatalog {

    /*@Override
    public LogicalNamespace getNamespace( long id ) {
        return new LogicalNamespace( 1, "private", 0, 0, "tester", NamespaceType.DOCUMENT, true );
    }


    @Override
    public CatalogUser getUser( long id ) {
        return new CatalogUser( 0, "name", "name" );
    }*/


    @Override
    public void change() {

    }


    @Override
    public <S extends AdapterCatalog> Optional<S> getAdapterCatalog( long id ) {
        return Optional.empty();
    }


    @Override
    public void addStoreSnapshot( AdapterCatalog snapshot ) {

    }


    @Override
    public long createAdapterTemplate( Class<? extends Adapter<?>> clazz, String adapterName, String description, List<DeployMode> modes, List<AbstractAdapterSetting> settings, Function4<Long, String, Map<String, String>, Adapter<?>> deployer ) {
        return 0;
    }


    @Override
    public void createInterfaceTemplate( String name, QueryInterfaceTemplate queryInterfaceTemplate ) {

    }


    @Override
    public void dropInterfaceTemplate( String name ) {

    }


    @Override
    public @NotNull Map<String, QueryInterfaceTemplate> getInterfaceTemplates() {
        return null;
    }


    @Override
    public Map<Long, AdapterTemplate> getAdapterTemplates() {
        return null;
    }


    @Override
    public void dropAdapterTemplate( long templateId ) {

    }


    @Override
    public PropertyChangeListener getChangeListener() {
        return null;
    }


    @Override
    public void restore() {

    }


    @Override
    public void attachCommitConstraint( Supplier<Boolean> constraintChecker, String description ) {

    }

}
