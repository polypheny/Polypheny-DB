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

package org.polypheny.db.policies.policy.selfadaptiveness;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.AdapterManager.AdapterInformation;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;

@Slf4j
public class SelfAdaptivAgent {


    private static SelfAdaptivAgent INSTANCE = null;


    public static SelfAdaptivAgent getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new SelfAdaptivAgent();
        }
        return INSTANCE;
    }


    public void initialize() {
        List<AdapterInformation> test = AdapterManager.getInstance().getAvailableAdapters( AdapterType.STORE );

        for ( AdapterInformation adapterInformation : test ) {
            log.warn( adapterInformation.name );

        }
    }

    @Getter
    public static class InformationContext {

        private List<Object> possibilities = null;
        private Class<?> clazz = null;
        @Setter
        private SchemaType nameSpaceModel;

        public void setPossibilities( List<Object> possibilities, Class<?> clazz ) {
            if ( this.possibilities != null ) {
                throw new RuntimeException( "Already set possibilities." );
            }
            this.possibilities = possibilities;
            this.clazz = clazz;
        }


    }

}
