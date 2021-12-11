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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.prepare;


import com.google.common.collect.ImmutableList;
import java.io.Reader;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgDecorrelator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.metadata.CachingAlgMetadataProvider;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.config.PolyphenyDbConnectionConfig;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.NodeToAlgConverter;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.languages.RexConvertletTable;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Context;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexExecutor;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgConversionException;
import org.polypheny.db.tools.FrameworkConfig;
import org.polypheny.db.tools.Frameworks;
import org.polypheny.db.tools.Planner;
import org.polypheny.db.tools.Program;
import org.polypheny.db.tools.ValidationException;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link Planner}.
 */
public class PlannerImpl implements Planner {

    private final OperatorTable operatorTable;
    private final ImmutableList<Program> programs;
    private final FrameworkConfig config;

    /**
     * Holds the trait definitions to be registered with planner. May be null.
     */
    private final ImmutableList<AlgTraitDef> traitDefs;

    private final ParserConfig parserConfig;
    private final NodeToAlgConverter.Config sqlToRelConverterConfig;
    private final RexConvertletTable convertletTable;

    private State state;

    // set in STATE_1_RESET
    private boolean open;

    // set in STATE_2_READY
    private SchemaPlus defaultSchema;
    private JavaTypeFactory typeFactory;
    private AlgOptPlanner planner;
    private RexExecutor executor;

    // set in STATE_4_VALIDATE
    private Validator validator;
    private Node validatedSqlNode;

    // set in STATE_5_CONVERT
    private AlgRoot root;


    /**
     * Creates a planner. Not a public API; call
     * {@link org.polypheny.db.tools.Frameworks#getPlanner} instead.
     */
    public PlannerImpl( FrameworkConfig config ) {
        this.config = config;
        this.defaultSchema = config.getDefaultSchema();
        this.operatorTable = config.getOperatorTable();
        this.programs = config.getPrograms();
        this.parserConfig = config.getParserConfig();
        this.sqlToRelConverterConfig = config.getSqlToRelConverterConfig();
        this.state = State.STATE_0_CLOSED;
        this.traitDefs = config.getTraitDefs();
        this.convertletTable = config.getConvertletTable();
        this.executor = config.getExecutor();
        reset();
    }


    /**
     * Makes sure that the state is at least the given state.
     */
    private void ensure( State state ) {
        if ( state == this.state ) {
            return;
        }
        if ( state.ordinal() < this.state.ordinal() ) {
            throw new IllegalArgumentException( "cannot move to " + state + " from " + this.state );
        }
        state.from( this );
    }


    @Override
    public AlgTraitSet getEmptyTraitSet() {
        return planner.emptyTraitSet();
    }


    @Override
    public void close() {
        open = false;
        typeFactory = null;
        state = State.STATE_0_CLOSED;
    }


    @Override
    public void reset() {
        ensure( State.STATE_0_CLOSED );
        open = true;
        state = State.STATE_1_RESET;
    }


    private void ready() {
        switch ( state ) {
            case STATE_0_CLOSED:
                reset();
        }
        ensure( State.STATE_1_RESET );
        Frameworks.withPlanner(
                ( cluster, algOptSchema, rootSchema ) -> {
                    Util.discard( rootSchema ); // use our own defaultSchema
                    typeFactory = (JavaTypeFactory) cluster.getTypeFactory();
                    planner = cluster.getPlanner();
                    planner.setExecutor( executor );
                    return null;
                },
                config );

        state = State.STATE_2_READY;

        // If user specify own traitDef, instead of default default trait, first, clear the default trait def registered with planner then,
        // register the trait def specified in traitDefs.
        if ( this.traitDefs != null ) {
            planner.clearRelTraitDefs();
            for ( AlgTraitDef def : this.traitDefs ) {
                planner.addAlgTraitDef( def );
            }
        }
    }


    @Override
    public Node parse( final Reader reader ) throws NodeParseException {
        switch ( state ) {
            case STATE_0_CLOSED:
            case STATE_1_RESET:
                ready();
        }
        ensure( State.STATE_2_READY );
        Parser parser = Parser.create( reader, parserConfig );
        Node sqlNode = parser.parseStmt();
        state = State.STATE_3_PARSED;
        return sqlNode;
    }


    @Override
    public Node validate( Node sqlNode ) throws ValidationException {
        ensure( State.STATE_3_PARSED );
        final Conformance conformance = conformance();
        final PolyphenyDbCatalogReader catalogReader = createCatalogReader();
        this.validator = LanguageManager.getInstance().createPolyphenyValidator( QueryLanguage.SQL, operatorTable, catalogReader, typeFactory, conformance );
        this.validator.setIdentifierExpansion( true );
        try {
            validatedSqlNode = validator.validate( sqlNode );
        } catch ( RuntimeException e ) {
            throw new ValidationException( e );
        }
        state = State.STATE_4_VALIDATED;
        return validatedSqlNode;
    }


    private Conformance conformance() {
        final Context context = config.getContext();
        if ( context != null ) {
            final PolyphenyDbConnectionConfig connectionConfig = context.unwrap( PolyphenyDbConnectionConfig.class );
            if ( connectionConfig != null ) {
                return connectionConfig.conformance();
            }
        }
        return config.getParserConfig().conformance();
    }


    @Override
    public Pair<Node, AlgDataType> validateAndGetType( Node sqlNode ) throws ValidationException {
        final Node validatedNode = this.validate( sqlNode );
        final AlgDataType type = this.validator.getValidatedNodeType( validatedNode );
        return Pair.of( validatedNode, type );
    }


    @Override
    public AlgRoot alg( Node sql ) throws AlgConversionException {
        ensure( State.STATE_4_VALIDATED );
        assert validatedSqlNode != null;
        final RexBuilder rexBuilder = createRexBuilder();
        final AlgOptCluster cluster = AlgOptCluster.create( planner, rexBuilder );
        final NodeToAlgConverter.Config config =
                new NodeToAlgConverter.ConfigBuilder()
                        .config( sqlToRelConverterConfig )
                        .trimUnusedFields( false )
                        .convertTableAccess( false )
                        .build();
        final NodeToAlgConverter sqlToRelConverter = LanguageManager.getInstance().createToRelConverter( QueryLanguage.SQL, validator, createCatalogReader(), cluster, convertletTable, config );
        root = sqlToRelConverter.convertQuery( validatedSqlNode, false, true );
        root = root.withAlg( sqlToRelConverter.flattenTypes( root.alg, true ) );
        final AlgBuilder algBuilder = config.getAlgBuilderFactory().create( cluster, null );
        root = root.withAlg( AlgDecorrelator.decorrelateQuery( root.alg, algBuilder ) );
        state = State.STATE_5_CONVERTED;
        return root;
    }


    // PolyphenyDbCatalogReader is stateless; no need to store one
    private PolyphenyDbCatalogReader createCatalogReader() {
        final SchemaPlus rootSchema = rootSchema( defaultSchema );
        return new PolyphenyDbCatalogReader(
                PolyphenyDbSchema.from( rootSchema ),
                PolyphenyDbSchema.from( defaultSchema ).path( null ),
                typeFactory );
    }


    private static SchemaPlus rootSchema( SchemaPlus schema ) {
        for ( ; ; ) {
            if ( schema.getParentSchema() == null ) {
                return schema;
            }
            schema = schema.getParentSchema();
        }
    }


    // RexBuilder is stateless; no need to store one
    private RexBuilder createRexBuilder() {
        return new RexBuilder( typeFactory );
    }


    @Override
    public JavaTypeFactory getTypeFactory() {
        return typeFactory;
    }


    @Override
    public AlgNode transform( int ruleSetIndex, AlgTraitSet requiredOutputTraits, AlgNode alg ) throws AlgConversionException {
        ensure( State.STATE_5_CONVERTED );
        alg.getCluster().setMetadataProvider(
                new CachingAlgMetadataProvider(
                        alg.getCluster().getMetadataProvider(),
                        alg.getCluster().getPlanner() ) );
        Program program = programs.get( ruleSetIndex );
        return program.run( planner, alg, requiredOutputTraits );
    }


    /**
     * Stage of a statement in the query-preparation lifecycle.
     */
    private enum State {
        STATE_0_CLOSED {
            @Override
            void from( PlannerImpl planner ) {
                planner.close();
            }
        },
        STATE_1_RESET {
            @Override
            void from( PlannerImpl planner ) {
                planner.ensure( STATE_0_CLOSED );
                planner.reset();
            }
        },
        STATE_2_READY {
            @Override
            void from( PlannerImpl planner ) {
                STATE_1_RESET.from( planner );
                planner.ready();
            }
        },
        STATE_3_PARSED,
        STATE_4_VALIDATED,
        STATE_5_CONVERTED;


        /**
         * Moves planner's state to this state. This must be a higher state.
         */
        void from( PlannerImpl planner ) {
            throw new IllegalArgumentException( "cannot move from " + planner.state + " to " + this );
        }
    }

}

