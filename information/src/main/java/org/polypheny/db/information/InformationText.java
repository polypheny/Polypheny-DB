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

package org.polypheny.db.information;


import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;
import lombok.Getter;


@Getter
public class InformationText extends Information {

    @JsonProperty
    private String text;


    public InformationText( final InformationGroup group, final String text ) {
        super( UUID.randomUUID().toString(), group.getId() );
        this.text = text;
    }


    public InformationText( final String id, final InformationGroup group ) {
        super( id, group.getId() );
    }


    public InformationText( final String id, final InformationGroup group, final String text ) {
        super( id, group.getId() );
        this.text = text;
    }

    public InformationText setText ( final String text ) {
        this.text = text;
        this.notifyManager();
        return this;
    }

}
