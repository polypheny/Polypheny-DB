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

public class BupInformationObject {

    //ImmutableMap<Long, LogicalNamespace> namespaces;
    @Getter @Setter
    List<LogicalNamespace> namespaces;
    @Getter @Setter
    List<LogicalNamespace> relNamespaces;
    @Getter @Setter
    List<LogicalNamespace> graphNamespaces;
    @Getter @Setter
    List<LogicalNamespace> docNamespaces;
    @Getter @Setter
    ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> bupRelNamespaces;
    @Getter @Setter
    ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> bupGraphNamespaces;
    @Getter @Setter
    ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> bupDocNamespaces;

    /*//TODO(FF): adjust (also to gather schema...there it is per table right now)
    @Getter @Setter
    List<LogicalView> views;
    @Getter @Setter
    List<LogicalMaterializedView> materializedViews;
     */

    //namespace id, list of entities for the namespace
    @Getter @Setter
    ImmutableMap<Long, List<LogicalView>> views;
    @Getter @Setter
    ImmutableMap<Long, List<BupSuperEntity<LogicalView>>> bupViews;
    @Getter @Setter
    ImmutableMap<Long, List<LogicalMaterializedView>> materializedViews;
    @Getter @Setter
    ImmutableMap<Long, List<BupSuperEntity<LogicalMaterializedView>>> bupMaterializedViews;
    @Getter @Setter
    ImmutableMap<Long, List<LogicalTable>> tables;
    @Getter @Setter
    ImmutableMap<Long, List<BupSuperEntity<LogicalTable>>> bupTables;
    @Getter @Setter
    ImmutableMap<Long, List<LogicalCollection>> collections;
    @Getter @Setter
    ImmutableMap<Long, List<BupSuperEntity<LogicalCollection>>> bupCollections;
    @Getter @Setter
    ImmutableMap<Long, LogicalGraph> graphs;
    @Getter @Setter
    ImmutableMap<Long, List<BupSuperEntity<LogicalGraph>>> bupGraphs;

    //table id, list of views for the table
    @Getter @Setter
    ImmutableMap<Long, List<LogicalColumn>> columns;
    @Getter @Setter
    ImmutableMap<Long, List<LogicalPrimaryKey>> primaryKeysPerTable;
    @Getter @Setter
    ImmutableMap<Long, List<LogicalForeignKey>> foreignKeysPerTable;
    @Getter @Setter
    ImmutableMap<Long, List<LogicalIndex>> logicalIndexes;
    @Getter @Setter
    ImmutableMap<Long, List<BupSuperEntity<LogicalIndex>>> bupLogicalIndexes;
    @Getter @Setter
    ImmutableMap<Long, List<LogicalConstraint>> constraints;

    @Getter @Setter
    Boolean collectedRelSchema = false;
    @Getter @Setter
    Boolean collectedDocSchema = false;
    @Getter @Setter
    Boolean collectedGraphSchema = false;


    public ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> transformNamespacesToBupSuperEntityMap( List<LogicalNamespace> namespaces, Boolean toBeInserted) {

        ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> resultMap;
        Map<Long, BupSuperEntity<LogicalNamespace>> tempNS = new HashMap<>();
        BupSuperEntity<LogicalNamespace> nsBupObj = new BupSuperEntity<>();

        for (LogicalNamespace ns : namespaces ) {
            nsBupObj.setEntityObject( ns );
            nsBupObj.setToBeInserted( toBeInserted );
            nsBupObj.setNameForQuery( ns.name );
            tempNS.put( ns.id, nsBupObj );
        }

        resultMap = ImmutableMap.copyOf( tempNS );
        return resultMap;
    }

    public ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> transformNamespacesToBupSuperEntityMap( List<LogicalNamespace> namespaces) {

        ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> resultMap;
        Map<Long, BupSuperEntity<LogicalNamespace>> tempNS = new HashMap<>();
        BupSuperEntity<LogicalNamespace> nsBupObj = new BupSuperEntity<>();

        for (LogicalNamespace ns : namespaces ) {
            nsBupObj.setEntityObject( ns );
            nsBupObj.setNameForQuery( ns.name );
            tempNS.put( ns.id, nsBupObj );
        }

        resultMap = ImmutableMap.copyOf( tempNS );
        return resultMap;
    }

    public ImmutableMap<Long, List<BupSuperEntity<LogicalEntity>>> transformLogicalEntitiesToBupSuperEntity( ImmutableMap<Long, List<LogicalEntity>> entityMap, Boolean toBeInserted) {

        ImmutableMap<Long, List<BupSuperEntity<LogicalEntity>>> resultMap;
        Map<Long, List<BupSuperEntity<LogicalEntity>>> tempMap = new HashMap<>();
        BupSuperEntity<LogicalEntity> tempBupEntity = new BupSuperEntity<>();

        //go through each element from entityMap, and for each list go through each element and transform it to a BupSuperEntity
        for ( Map.Entry<Long, List<LogicalEntity>> entry : entityMap.entrySet() ) {
            List<LogicalEntity> entityList = entry.getValue();
            List<BupSuperEntity<LogicalEntity>> bupEntityList = new ArrayList<>();

            for ( LogicalEntity entity : entityList ) {
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

    public ImmutableMap<Long, List<BupSuperEntity<LogicalEntity>>> transformLogicalEntitiesToBupSuperEntity( ImmutableMap<Long, List<LogicalEntity>> entityMap) {

        ImmutableMap<Long, List<BupSuperEntity<LogicalEntity>>> resultMap;
        Map<Long, List<BupSuperEntity<LogicalEntity>>> tempMap = new HashMap<>();
        BupSuperEntity<LogicalEntity> tempBupEntity = new BupSuperEntity<>();

        //go through each element from entityMap, and for each list go through each element and transform it to a BupSuperEntity
        for ( Map.Entry<Long, List<LogicalEntity>> entry : entityMap.entrySet() ) {
            List<LogicalEntity> entityList = entry.getValue();
            List<BupSuperEntity<LogicalEntity>> bupEntityList = new ArrayList<>();

            for ( LogicalEntity entity : entityList ) {
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

}
