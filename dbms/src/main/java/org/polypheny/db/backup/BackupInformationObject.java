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

package org.polypheny.db.backup;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.logical.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.util.Pair;

@Getter @Setter
public class BackupInformationObject {

    //ImmutableMap<Long, LogicalNamespace> namespaces;
    private List<LogicalNamespace> namespaces;

    private List<LogicalNamespace> relNamespaces;

    private List<LogicalNamespace> graphNamespaces;

    private List<LogicalNamespace> docNamespaces;

    //private ImmutableMap<Long, BackupEntityWrapper<LogicalNamespace>> bupRelNamespaces;
    //private ImmutableMap<Long, BackupEntityWrapper<LogicalNamespace>> bupGraphNamespaces;
    //private ImmutableMap<Long, BackupEntityWrapper<LogicalNamespace>> bupDocNamespaces;

    private ImmutableMap<Long, BackupEntityWrapper<LogicalNamespace>> wrappedNamespaces;

    /*//TODO(FF): adjust (also to gather schema...there it is per table right now)
    @Getter @Setter
    List<LogicalView> views;
    @Getter @Setter
    List<LogicalMaterializedView> materializedViews;
     */

    //namespace id, list of entities for the namespace
    private ImmutableMap<Long, List<LogicalView>> views;

    private ImmutableMap<Long, List<BackupEntityWrapper<LogicalView>>> wrappedViews;

    private ImmutableMap<Long, List<LogicalMaterializedView>> materializedViews;

    private ImmutableMap<Long, List<BackupEntityWrapper<LogicalMaterializedView>>> wrappedMaterializedViews;

    private ImmutableMap<Long, List<LogicalTable>> tables;

    private ImmutableMap<Long, List<BackupEntityWrapper<LogicalTable>>> wrappedTables;

    private ImmutableMap<Long, List<LogicalCollection>> collections;

    private ImmutableMap<Long, List<BackupEntityWrapper<LogicalCollection>>> wrappeedCollections;

    private ImmutableMap<Long, LogicalGraph> graphs;

    private ImmutableMap<Long, List<BackupEntityWrapper<LogicalGraph>>> wrappedGraphs;

    //table id, list of views for the table
    private ImmutableMap<Long, List<LogicalColumn>> columns;

    private ImmutableMap<Long, List<LogicalPrimaryKey>> primaryKeysPerTable;

    private ImmutableMap<Long, List<LogicalForeignKey>> foreignKeysPerTable;

    private ImmutableMap<Long, List<LogicalIndex>> logicalIndexes;

    private ImmutableMap<Long, List<BackupEntityWrapper<LogicalIndex>>> wrappedLogicalIndexes;

    private ImmutableMap<Long, List<LogicalConstraint>> constraints;


    private Boolean collectedRelSchema = false;

    private Boolean collectedDocSchema = false;

    private Boolean collectedGraphSchema = false;


    public ImmutableMap<Long, BackupEntityWrapper<LogicalNamespace>> wrapNamespaces( List<LogicalNamespace> namespaces, Map<Long, List<Long>> namespaceDependencies, Map<Long, List<Long>> tableDependencies, Map<Long, List<Pair<Long, Long>>> namespaceTableDependencies, Boolean toBeInserted ) {

        ImmutableMap<Long, BackupEntityWrapper<LogicalNamespace>> resultMap;
        Map<Long, BackupEntityWrapper<LogicalNamespace>> tempNS = new HashMap<>();
        BackupEntityWrapper<LogicalNamespace> nsBupObj = new BackupEntityWrapper<>();

        for ( LogicalNamespace ns : namespaces ) {
            nsBupObj.setEntityObject( ns );
            nsBupObj.setToBeInserted( toBeInserted );
            nsBupObj.setNameForQuery( ns.name );
            tempNS.put( ns.id, nsBupObj );
        }

        resultMap = ImmutableMap.copyOf( tempNS );
        return resultMap;
    }


    public ImmutableMap<Long, BackupEntityWrapper<LogicalNamespace>> wrapNamespaces( List<LogicalNamespace> namespaces, Map<Long, List<Long>> namespaceDependencies, Map<Long, List<Long>> tableDependencies, Map<Long, List<Pair<Long, Long>>> namespaceTableDependencies ) {

        ImmutableMap<Long, BackupEntityWrapper<LogicalNamespace>> resultMap;
        Map<Long, BackupEntityWrapper<LogicalNamespace>> tempNS = new HashMap<>();

        for ( LogicalNamespace ns : namespaces ) {
            BackupEntityWrapper<LogicalNamespace> nsBupObj = new BackupEntityWrapper<>();
            nsBupObj.setEntityObject( ns );
            nsBupObj.setNameForQuery( ns.name );

            // create entityReferences for each namespace (if there is a reference) with namespacedependencies, and add entityReferences to the backupinformationobject
            if ( namespaceDependencies.containsKey( ns.id ) || namespaceTableDependencies.containsKey( ns.id ) ) {
                EntityReferencer entityReferencer = new EntityReferencer( ns.id, BackupEntityType.NAMESPACE );
                if ( namespaceDependencies.containsKey( ns.id ) ) {
                    entityReferencer.setReferencerNamespaces( namespaceDependencies.get( ns.id ) );
                }
                if ( namespaceTableDependencies.containsKey( ns.id )) {
                    entityReferencer.setReferencerNamespaceTablePairs( namespaceTableDependencies.get( ns.id ) );

                    // get out all the tableIds from the pairs and add them to the referencerTables list
                    List<Long> tempReferencerTables = new ArrayList<>();
                    for ( Pair<Long, Long> pair : namespaceTableDependencies.get( ns.id ) ) {
                        tempReferencerTables.add( pair.left );
                    }
                    entityReferencer.setReferencerTables( tempReferencerTables );
                }
                nsBupObj.setEntityReferencer( entityReferencer );
            }

            tempNS.put( ns.id, nsBupObj );
        }

        resultMap = ImmutableMap.copyOf( tempNS );
        return resultMap;
    }


    public ImmutableMap<Long, List<BackupEntityWrapper<LogicalEntity>>> wrapLogicalEntities( ImmutableMap<Long, List<LogicalEntity>> entityMap, Boolean toBeInserted ) {

        ImmutableMap<Long, List<BackupEntityWrapper<LogicalEntity>>> resultMap;
        Map<Long, List<BackupEntityWrapper<LogicalEntity>>> tempMap = new HashMap<>();

        //go through each element from entityMap, and for each list go through each element and transform it to a BupSuperEntity
        for ( Map.Entry<Long, List<LogicalEntity>> entry : entityMap.entrySet() ) {
            List<LogicalEntity> entityList = entry.getValue();
            List<BackupEntityWrapper<LogicalEntity>> bupEntityList = new ArrayList<>();

            for ( LogicalEntity entity : entityList ) {
                BackupEntityWrapper<LogicalEntity> tempBupEntity = new BackupEntityWrapper<>();
                tempBupEntity.setEntityObject( entity );
                tempBupEntity.setToBeInserted( toBeInserted );
                tempBupEntity.setNameForQuery( entity.name );
                bupEntityList.add( tempBupEntity );
            }
            tempMap.put( entry.getKey(), bupEntityList );

        }

        resultMap = ImmutableMap.copyOf( tempMap );
        return resultMap;
    }


    public ImmutableMap<Long, List<BackupEntityWrapper<LogicalEntity>>> wrapLogicalEntities( ImmutableMap<Long, List<LogicalEntity>> entityMap ) {

        ImmutableMap<Long, List<BackupEntityWrapper<LogicalEntity>>> resultMap;
        Map<Long, List<BackupEntityWrapper<LogicalEntity>>> tempMap = new HashMap<>();

        //go through each element from entityMap, and for each list go through each element and transform it to a BupSuperEntity
        for ( Map.Entry<Long, List<LogicalEntity>> entry : entityMap.entrySet() ) {
            List<LogicalEntity> entityList = entry.getValue();
            List<BackupEntityWrapper<LogicalEntity>> bupEntityList = new ArrayList<>();

            for ( LogicalEntity entity : entityList ) {
                BackupEntityWrapper<LogicalEntity> tempBupEntity = new BackupEntityWrapper<>();
                tempBupEntity.setEntityObject( entity );
                tempBupEntity.setToBeInserted( true );
                tempBupEntity.setNameForQuery( entity.name );
                bupEntityList.add( tempBupEntity );
            }
            tempMap.put( entry.getKey(), bupEntityList );

        }

        resultMap = ImmutableMap.copyOf( tempMap );
        return resultMap;
    }


    public ImmutableMap<Long, List<BackupEntityWrapper<LogicalTable>>> tempWrapLogicalTables( ImmutableMap<Long, List<LogicalTable>> entityMap, Map<Long, List<Long>> tableDependencies, Map<Long, List<Pair<Long, Long>>> namespaceTableDependendencies, Boolean toBeInserted ) {

        ImmutableMap<Long, List<BackupEntityWrapper<LogicalTable>>> resultMap;
        Map<Long, List<BackupEntityWrapper<LogicalTable>>> tempMap = new HashMap<>();

        //go through each element from entityMap, and for each list go through each element and transform it to a BupSuperEntity
        for ( Map.Entry<Long, List<LogicalTable>> entry : entityMap.entrySet() ) {
            List<LogicalTable> entityList = entry.getValue();
            List<BackupEntityWrapper<LogicalTable>> bupEntityList = new ArrayList<>();

            for ( LogicalTable entity : entityList ) {
                BackupEntityWrapper<LogicalTable> tempBupEntity = new BackupEntityWrapper<>();
                tempBupEntity.setEntityObject( entity );
                tempBupEntity.setToBeInserted( toBeInserted );
                tempBupEntity.setNameForQuery( entity.name );
                bupEntityList.add( tempBupEntity );

                // create entityReferences for each table (if there is a reference) with tableDependencies, and add entityReferences to the backupinformationobject
                if (entity.getEntityType().equals( EntityType.ENTITY)) {
                    EntityReferencer entityReferencer = new EntityReferencer( entity.getId(), BackupEntityType.TABLE );
                    if (tableDependencies.containsKey( entity.getId() )) {
                        entityReferencer.setReferencerTables( tableDependencies.get( entity.getId() ) );
                    }
                    if (namespaceTableDependendencies.containsKey( entity.getId() )) {
                        entityReferencer.setReferencerNamespaceTablePairs( namespaceTableDependendencies.get( entity.getId() ) );
                    }
                    tempBupEntity.setEntityReferencer( entityReferencer );
                }


            }

            tempMap.put( entry.getKey(), bupEntityList );

        }

        resultMap = ImmutableMap.copyOf( tempMap );
        return resultMap;
    }


    public void transformationManager() {
        //ImmutableMap<Long, List<LogicalEntity>> entityMap = getTables();
        //transformLogicalEntitiesToBupSuperEntity( getTables(), true );
        //TODO: testen ob es mit ganzem angabekladatch funktioniert (past me, what?)
    }


    public List<BackupEntityWrapper<LogicalEntity>> wrapLogicalEntity( List<LogicalEntity> entityList ) {

        //go through each element from entityMap, and for each list go through each element and transform it to a BupSuperEntity
        List<BackupEntityWrapper<LogicalEntity>> bupEntityList = new ArrayList<>();

        for ( LogicalEntity entity : entityList ) {
            BackupEntityWrapper<LogicalEntity> tempBupEntity = new BackupEntityWrapper<>();
            tempBupEntity.setEntityObject( entity );
            tempBupEntity.setToBeInserted( true );
            tempBupEntity.setNameForQuery( entity.name );
            bupEntityList.add( tempBupEntity );
        }

        return bupEntityList;

    }

}
