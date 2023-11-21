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
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.logical.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BupInformationObject {

    //ImmutableMap<Long, LogicalNamespace> namespaces;
    @Getter
    @Setter
    private List<LogicalNamespace> namespaces;
    @Getter
    @Setter
    private List<LogicalNamespace> relNamespaces;
    @Getter
    @Setter
    private List<LogicalNamespace> graphNamespaces;
    @Getter
    @Setter
    private List<LogicalNamespace> docNamespaces;
    @Getter
    @Setter
    private ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> bupRelNamespaces;
    @Getter
    @Setter
    private ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> bupGraphNamespaces;
    @Getter
    @Setter
    private ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> bupDocNamespaces;
    @Getter
    @Setter
    private ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> bupNamespaces;

    /*//TODO(FF): adjust (also to gather schema...there it is per table right now)
    @Getter @Setter
    List<LogicalView> views;
    @Getter @Setter
    List<LogicalMaterializedView> materializedViews;
     */
    //TODO(FF): make it private(all)
    //namespace id, list of entities for the namespace
    @Getter
    @Setter
    private ImmutableMap<Long, List<LogicalView>> views;
    @Getter
    @Setter
    private ImmutableMap<Long, List<BupSuperEntity<LogicalView>>> bupViews;
    @Getter
    @Setter
    private ImmutableMap<Long, List<LogicalMaterializedView>> materializedViews;
    @Getter
    @Setter
    private ImmutableMap<Long, List<BupSuperEntity<LogicalMaterializedView>>> bupMaterializedViews;
    @Getter
    @Setter
    private ImmutableMap<Long, List<LogicalTable>> tables;
    @Getter
    @Setter
    private ImmutableMap<Long, List<BupSuperEntity<LogicalTable>>> bupTables;
    @Getter
    @Setter
    private ImmutableMap<Long, List<LogicalCollection>> collections;
    @Getter
    @Setter
    private ImmutableMap<Long, List<BupSuperEntity<LogicalCollection>>> bupCollections;
    @Getter
    @Setter
    private ImmutableMap<Long, LogicalGraph> graphs;
    @Getter
    @Setter
    private ImmutableMap<Long, List<BupSuperEntity<LogicalGraph>>> bupGraphs;

    //table id, list of views for the table
    @Getter
    @Setter
    private ImmutableMap<Long, List<LogicalColumn>> columns;
    @Getter
    @Setter
    private ImmutableMap<Long, List<LogicalPrimaryKey>> primaryKeysPerTable;
    @Getter
    @Setter
    private ImmutableMap<Long, List<LogicalForeignKey>> foreignKeysPerTable;
    @Getter
    @Setter
    private ImmutableMap<Long, List<LogicalIndex>> logicalIndexes;
    @Getter
    @Setter
    private ImmutableMap<Long, List<BupSuperEntity<LogicalIndex>>> bupLogicalIndexes;
    @Getter
    @Setter
    private ImmutableMap<Long, List<LogicalConstraint>> constraints;

    @Getter
    @Setter
    private Boolean collectedRelSchema = false;
    @Getter
    @Setter
    private Boolean collectedDocSchema = false;
    @Getter
    @Setter
    private Boolean collectedGraphSchema = false;


    public ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> transformNamespacesToBupSuperEntityMap( List<LogicalNamespace> namespaces, Boolean toBeInserted ) {

        ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> resultMap;
        Map<Long, BupSuperEntity<LogicalNamespace>> tempNS = new HashMap<>();
        BupSuperEntity<LogicalNamespace> nsBupObj = new BupSuperEntity<>();

        for ( LogicalNamespace ns : namespaces ) {
            nsBupObj.setEntityObject( ns );
            nsBupObj.setToBeInserted( toBeInserted );
            nsBupObj.setNameForQuery( ns.name );
            tempNS.put( ns.id, nsBupObj );
        }

        resultMap = ImmutableMap.copyOf( tempNS );
        return resultMap;
    }


    public ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> transformNamespacesToBupSuperEntityMap( List<LogicalNamespace> namespaces ) {

        ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> resultMap;
        Map<Long, BupSuperEntity<LogicalNamespace>> tempNS = new HashMap<>();

        for ( LogicalNamespace ns : namespaces ) {
            BupSuperEntity<LogicalNamespace> nsBupObj = new BupSuperEntity<>();
            nsBupObj.setEntityObject( ns );
            nsBupObj.setNameForQuery( ns.name );
            tempNS.put( ns.id, nsBupObj );
        }

        resultMap = ImmutableMap.copyOf( tempNS );
        return resultMap;
    }


    public ImmutableMap<Long, List<BupSuperEntity<LogicalEntity>>> transformLogicalEntitiesToBupSuperEntity( ImmutableMap<Long, List<LogicalEntity>> entityMap, Boolean toBeInserted ) {

        ImmutableMap<Long, List<BupSuperEntity<LogicalEntity>>> resultMap;
        Map<Long, List<BupSuperEntity<LogicalEntity>>> tempMap = new HashMap<>();

        //go through each element from entityMap, and for each list go through each element and transform it to a BupSuperEntity
        for ( Map.Entry<Long, List<LogicalEntity>> entry : entityMap.entrySet() ) {
            List<LogicalEntity> entityList = entry.getValue();
            List<BupSuperEntity<LogicalEntity>> bupEntityList = new ArrayList<>();

            for ( LogicalEntity entity : entityList ) {
                BupSuperEntity<LogicalEntity> tempBupEntity = new BupSuperEntity<>();
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


    public ImmutableMap<Long, List<BupSuperEntity<? extends LogicalEntity>>> transformLogicalEntitiesToBupSuperEntityyy( ImmutableMap<Long, List<? extends LogicalEntity>> entityMap ) {

        ImmutableMap<Long, List<BupSuperEntity<? extends LogicalEntity>>> resultMap;
        Map<Long, List<BupSuperEntity<LogicalEntity>>> tempMap = new HashMap<>();

        //go through each element from entityMap, and for each list go through each element and transform it to a BupSuperEntity
        for ( Map.Entry<Long, List<? extends LogicalEntity>> entry : entityMap.entrySet() ) {
            List<? extends LogicalEntity> entityList = entry.getValue();
            List<BupSuperEntity<LogicalEntity>> bupEntityList = new ArrayList<>();

            for ( LogicalEntity entity : entityList ) {
                BupSuperEntity<LogicalEntity> tempBupEntity = new BupSuperEntity<>();
                tempBupEntity.setEntityObject( entity );
                tempBupEntity.setToBeInserted( true );
                tempBupEntity.setNameForQuery( entity.name );
                bupEntityList.add( tempBupEntity );
            }
            tempMap.put( entry.getKey(), bupEntityList );

        }

        resultMap = ImmutableMap.copyOf( (Map<? extends Long, ? extends List<BupSuperEntity<? extends LogicalEntity>>>) tempMap );
        return resultMap;
    }


    public ImmutableMap<Long, List<BupSuperEntity<LogicalTable>>> tempTableTransformation( ImmutableMap<Long, List<LogicalTable>> entityMap, Boolean toBeInserted ) {

        ImmutableMap<Long, List<BupSuperEntity<LogicalTable>>> resultMap;
        Map<Long, List<BupSuperEntity<LogicalTable>>> tempMap = new HashMap<>();

        //go through each element from entityMap, and for each list go through each element and transform it to a BupSuperEntity
        for ( Map.Entry<Long, List<LogicalTable>> entry : entityMap.entrySet() ) {
            List<LogicalTable> entityList = entry.getValue();
            List<BupSuperEntity<LogicalTable>> bupEntityList = new ArrayList<>();

            for ( LogicalTable entity : entityList ) {
                BupSuperEntity<LogicalTable> tempBupEntity = new BupSuperEntity<>();
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


    public void transformationManager() {
        //ImmutableMap<Long, List<LogicalEntity>> entityMap = getTables();
        //transformLogicalEntitiesToBupSuperEntity( getTables(), true );
        //TODO: testen ob es mit ganzem angabekladatch funktioniert (past me, what?)
    }


    public List<BupSuperEntity<LogicalEntity>> transformLogigalEntityToSuperEntity( List<LogicalEntity> entityList ) {

        //go through each element from entityMap, and for each list go through each element and transform it to a BupSuperEntity
        List<BupSuperEntity<LogicalEntity>> bupEntityList = new ArrayList<>();

        for ( LogicalEntity entity : entityList ) {
            BupSuperEntity<LogicalEntity> tempBupEntity = new BupSuperEntity<>();
            tempBupEntity.setEntityObject( entity );
            tempBupEntity.setToBeInserted( true );
            tempBupEntity.setNameForQuery( entity.name );
            bupEntityList.add( tempBupEntity );
        }

        return bupEntityList;

    }

}
