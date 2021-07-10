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

package org.polypheny.db.rel.logical;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.bson.types.ObjectId;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.document.DocumentTypeUtil;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelCollationTraitDef;
import org.polypheny.db.rel.RelInput;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Documents;
import org.polypheny.db.rel.metadata.RelMdCollation;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelDataTypeFieldImpl;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rel.type.RelRecordType;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.sql.SqlCollation;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.NlsString;

public class LogicalDocuments extends LogicalValues implements Documents {

    private final static PolyTypeFactoryImpl typeFactory = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
    private final static Gson gson = new Gson();

    @Getter
    private final List<RelDataType> rowTypes;
    @Getter
    private final RelDataType defaultRowType;
    @Getter
    private final ImmutableList<ImmutableList<RexLiteral>> normalizedTuples;


    /**
     * Creates an <code>AbstractRelNode</code>.
     *
     * @param cluster
     * @param rowTypes
     * @param defaultRowType
     * @param traitSet
     * @param tuples
     */
    public LogicalDocuments( RelOptCluster cluster, List<RelDataType> rowTypes, RelDataType defaultRowType, RelTraitSet traitSet, ImmutableList<ImmutableList<RexLiteral>> tuples, ImmutableList<ImmutableList<RexLiteral>> normalizedTuples ) {
        super( cluster, traitSet, defaultRowType, normalizedTuples );
        this.tuples = tuples;
        this.rowTypes = rowTypes;
        this.defaultRowType = defaultRowType;
        this.normalizedTuples = normalizedTuples;
    }


    public static RelNode create( RelOptCluster cluster, final List<RelDataType> rowTypes, final ImmutableList<ImmutableList<RexLiteral>> tuples ) {
        List<RelDataTypeField> fields = new ArrayList<>();
        fields.add( new RelDataTypeFieldImpl( "_id", 0, typeFactory.createPolyType( PolyType.VARCHAR, 24 ) ) );
        fields.add( new RelDataTypeFieldImpl( "_data", 1, typeFactory.createPolyType( PolyType.VARCHAR, 1024 ) ) );
        RelDataType defaultRowType = new RelRecordType( fields );

        ImmutableList<ImmutableList<RexLiteral>> normalizedTuples = normalize( tuples, rowTypes, defaultRowType );

        return create( cluster, rowTypes, tuples, defaultRowType, normalizedTuples );
    }


    public static RelNode create( RelOptCluster cluster, List<RelDataType> rowTypes, ImmutableList<ImmutableList<RexLiteral>> tuples, RelDataType defaultRowType, ImmutableList<ImmutableList<RexLiteral>> normalizedTuples ) {
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet = cluster.traitSetOf( Convention.NONE )
                .replaceIfs( RelCollationTraitDef.INSTANCE, () -> RelMdCollation.values( mq, defaultRowType, normalizedTuples ) );
        return new LogicalDocuments( cluster, rowTypes, defaultRowType, traitSet, tuples, normalizedTuples );
    }


    @Override
    public SchemaType getModel() {
        return SchemaType.DOCUMENT;
    }


    @Override
    public ImmutableList<ImmutableList<RexLiteral>> getTuples( RelInput input ) {
        return super.getTuples( input );
    }


    @Override
    public ImmutableList<ImmutableList<RexLiteral>> getTuples() {
        return getNormalizedTuples();
    }


    @Override
    public ImmutableList<ImmutableList<RexLiteral>> getFlatTuples() {
        return this.tuples;
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        assert inputs.isEmpty();
        return new LogicalDocuments( getCluster(), rowTypes, defaultRowType, traitSet, tuples, normalizedTuples );
    }


    /**
     * Brings the document structured tuple into a fixed structure, which also non-document stores can handle
     *
     * @return
     */
    private static ImmutableList<ImmutableList<RexLiteral>> normalize( ImmutableList<ImmutableList<RexLiteral>> tuples, List<RelDataType> rowTypes, RelDataType defaultRowType ) {
        List<ImmutableList<RexLiteral>> normalized = new ArrayList<>();
        List<String> rowNames = defaultRowType.getFieldNames();

        int pos = 0;
        for ( ImmutableList<RexLiteral> tuple : tuples ) {
            List<RexLiteral> normalizedTuple = new ArrayList<>();
            List<String> fieldNames = rowTypes.get( pos ).getFieldNames();
            Map<String, Comparable<?>> data = new HashMap<>();
            List<String> usedNames = new ArrayList<>();

            int fieldPos = 0;
            for ( RexLiteral literal : tuple ) {
                String name = fieldNames.get( fieldPos );
                if ( rowNames.contains( name ) ) {
                    normalizedTuple.add( literal );
                    usedNames.add( name );
                } else {
                    data.put( name, DocumentTypeUtil.getMqlType( literal ) );
                }
                fieldPos++;
            }

            // we have to adjust and fit to the provided defaultRowType, if we have different "dynamic" columns in the future
            if ( !usedNames.contains( "_id" ) ) {
                normalizedTuple.add( 0, new RexLiteral( new NlsString( ObjectId.get().toString(), "ISO-8859-1", SqlCollation.IMPLICIT ), typeFactory.createPolyType( PolyType.CHAR, 24 ), PolyType.CHAR ) );
            }
            String parsed = gson.toJson( data );
            RexLiteral literal = new RexLiteral( new NlsString( parsed, "ISO-8859-1", SqlCollation.IMPLICIT ), typeFactory.createPolyType( PolyType.CHAR, parsed.length() ), PolyType.CHAR );
            normalizedTuple.add( literal );

            pos++;
            normalized.add( ImmutableList.copyOf( normalizedTuple ) );
        }

        return ImmutableList.copyOf( normalized );
    }


}
