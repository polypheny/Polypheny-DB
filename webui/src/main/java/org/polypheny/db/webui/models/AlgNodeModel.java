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

package org.polypheny.db.webui.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Intersect;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Minus;
import org.polypheny.db.algebra.core.ModifyCollect;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.core.document.DocumentAlg;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.core.relational.RelAlg;

@Value
public class AlgNodeModel {

    @JsonProperty
    String name;

    @JsonProperty
    @NonFinal
    int inputs;

    @JsonProperty
    @NonFinal
    String icon;

    @JsonProperty
    @NonFinal
    String symbol;

    @JsonProperty
    @NonFinal
    String type;

    @JsonProperty
    @NonFinal
    String model;


    public AlgNodeModel( String name ) {
        this.name = name.replace( "Logical", "" ).replace( "Rel", "" );
    }


    public static AlgNodeModel from( Class<? extends AlgNode> nodeClass ) {
        AlgNodeModel model = new AlgNodeModel( nodeClass.getSimpleName() );

        if ( SingleAlg.class.isAssignableFrom( nodeClass ) ) {
            model.inputs = 1;
        } else if ( BiAlg.class.isAssignableFrom( nodeClass ) ) {
            model.inputs = 2;
        } else {
            model.inputs = 0;
        }

        if ( RelAlg.class.isAssignableFrom( nodeClass ) ) {
            model.model = "relational";
        } else if ( DocumentAlg.class.isAssignableFrom( nodeClass ) ) {
            model.model = "document";
        } else if ( LpgAlg.class.isAssignableFrom( nodeClass ) ) {
            model.model = "graph";
        } else {
            model.model = "common";
        }

        if ( Project.class.isAssignableFrom( nodeClass ) ) {
            model.symbol = "&pi;";
            model.type = Project.class.getSimpleName();
        } else if ( Filter.class.isAssignableFrom( nodeClass ) ) {
            model.symbol = "&sigma;";
            model.type = Filter.class.getSimpleName();
        } else if ( Scan.class.isAssignableFrom( nodeClass ) ) {
            model.icon = "fa fa-database";
            model.type = Scan.class.getSimpleName();
        } else if ( Join.class.isAssignableFrom( nodeClass ) ) {
            model.symbol = "&#8904;";
            model.type = Join.class.getSimpleName();
        } else if ( Aggregate.class.isAssignableFrom( nodeClass ) ) {
            model.icon = "fa fa-plus-circle";
            model.type = Aggregate.class.getSimpleName();
        } else if ( Union.class.isAssignableFrom( nodeClass ) ) {
            model.symbol = "&cup;";
            model.type = Union.class.getSimpleName();
        } else if ( Sort.class.isAssignableFrom( nodeClass ) ) {
            model.icon = "fa fa-arrows-v";
            model.type = Sort.class.getSimpleName();
        } else if ( Minus.class.isAssignableFrom( nodeClass ) ) {
            model.icon = "fa fa-minus-circle";
            model.type = Minus.class.getSimpleName();
        } else if ( Intersect.class.isAssignableFrom( nodeClass ) ) {
            model.symbol = "&cap;";
            model.type = Intersect.class.getSimpleName();
        } else if ( Modify.class.isAssignableFrom( nodeClass ) ) {
            model.icon = "fa fa-wrench";
            model.type = Modify.class.getSimpleName();
        } else if ( ModifyCollect.class.isAssignableFrom( nodeClass ) ) {
            model.icon = "fa fa-wrench";
            model.type = ModifyCollect.class.getSimpleName();
        } else {
            model.icon = "fa fa-arrows";
            model.type = AlgNode.class.getSimpleName();
        }

        return model;

    }


}
