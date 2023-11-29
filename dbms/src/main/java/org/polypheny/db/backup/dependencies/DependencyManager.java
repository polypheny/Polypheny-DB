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

package org.polypheny.db.backup.dependencies;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.backup.BackupInformationObject;
import org.polypheny.db.util.Pair;

public class DependencyManager {
    
    
    public List<Long> getReferencedEntities( EntityReferencer entityReferencer, List<EntityReferencer> allTableReferencers ) {
        DependencyAssembler dependencyAssembler = new DependencyAssembler();
        List<Long> visited = new ArrayList<>();
        //fixme: check if entityReferencer is empty/null
        
        if ( entityReferencer.getEntityType().equals( BackupEntityType.NAMESPACE ) ) {
            //TODO(FF): implement - same as the rest, but remove (or handle differently) first element (first element should be the namespace itself)
            // go through all tables referenced by namespace, do recursive function for each table, but manually check here if it is already visited for the outermost layer


            //collect all tables from referencerNamespaceTablePairs where the key is the id of the entityReferencer
            List<EntityReferencer> referencerTables = new ArrayList<>();
            for ( EntityReferencer namespaceReferencer : allTableReferencers ) {
                for ( Pair<Long, Long> pair : namespaceReferencer.getReferencerNamespaceTablePairs() ) {
                    if ( pair.left.equals( entityReferencer.getEntityId() ) ) {
                        Long tableId = pair.right;
                        //find the entityReferencer for the tableId in allTableReferencers
                        for ( EntityReferencer tableReferencer : allTableReferencers ) {
                            if ( tableReferencer.getEntityId().equals( tableId ) ) {
                                referencerTables.add( tableReferencer );
                            }
                        }
                    }
                }
            }

            for ( EntityReferencer nextEntity : referencerTables ) {
                visited = dependencyAssembler.getDependencies( visited, nextEntity, allTableReferencers );
            }
            return entityReferencer.getReferencerNamespaces();


        } else if ( entityReferencer.getEntityType().equals( BackupEntityType.TABLE ) ) {
            return dependencyAssembler.getDependencies( visited, entityReferencer, allTableReferencers );
        } else {
            throw new RuntimeException( "Unknown entity type" );
        }
    }



}
