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
import java.util.stream.Collectors;

public class DependencyAssembler {

    public List<Long> getDependencies( List<Long> visited, EntityReferencer currentEntity, List<EntityReferencer> allTableReferencers ) {
        if ( (visited.contains( currentEntity.getEntityId() )) || (currentEntity.getReferencerTables().isEmpty() && currentEntity.getReferencerNamespaces().isEmpty()) ) {
            return visited;
        } else {
            visited.add( currentEntity.getEntityId() );


            // List<EntityReferencer> referencerTables = allTableReferencers.stream().filter( entityReferencer -> entityReferencer.getEntityId().equals( currentEntity.getEntityId() ) ).toList();
            /*
            List<EntityReferencer> referencerTables = new ArrayList<>();
            for ( EntityReferencer entityReferencer : allTableReferencers ) {
                if ( currentEntity.getReferencerTables().contains( entityReferencer.getEntityId() ) ) {
                    referencerTables.add( entityReferencer );
                }
            }
             */

            List<EntityReferencer> referencerTables = allTableReferencers.stream().filter( e -> e.getEntityId().equals( currentEntity.getEntityId() )).collect( Collectors.toList());
            for ( EntityReferencer nextEntity : referencerTables ) {
                visited = getDependencies( visited, nextEntity, allTableReferencers );
            }

            return visited;
        }
        
    }

}
