/*
 * Copyright 2019-2020 The Polypheny Project
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

package ch.unibas.dmi.dbis.polyphenydb.prepare;


import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfig;
import ch.unibas.dmi.dbis.polyphenydb.plan.Context;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable.ViewExpander;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelRoot;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.CachingRelMetadataProvider;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexExecutor;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParseException;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser.SqlParserConfig;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformance;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.RelDecorrelator;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.SqlRexConvertletTable;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.SqlToRelConverter;
import ch.unibas.dmi.dbis.polyphenydb.tools.FrameworkConfig;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.tools.Planner;
import ch.unibas.dmi.dbis.polyphenydb.tools.Program;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelConversionException;
import ch.unibas.dmi.dbis.polyphenydb.tools.ValidationException;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import java.io.Reader;
import java.util.List;


/**
 * Implementation of {@link Planner}.
 */
public class PlannerImpl implements Planner, ViewExpander {

    private final SqlOperatorTable operatorTable;
    private final ImmutableList<Program> programs;
    private final FrameworkConfig config;

    /**
     * Holds the trait definitions to be registered with planner. May be null.
     */
    private final ImmutableList<RelTraitDef> traitDefs;

    private final SqlParserConfig parserConfig;
    private final SqlToRelConverter.Config sqlToRelConverterConfig;
    private final SqlRexConvertletTable convertletTable;

    private State state;

    // set in STATE_1_RESET
    private boolean open;

    // set in STATE_2_READY
    private SchemaPlus defaultSchema;
    private JavaTypeFactory typeFactory;
    private RelOptPlanner planner;
    private RexExecutor executor;

    // set in STATE_4_VALIDATE
    private PolyphenyDbSqlValidator validator;
    private SqlNode validatedSqlNode;

    // set in STATE_5_CONVERT
    private RelRoot root;


    /**
     * Creates a planner. Not a public API; call
     * {@link ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks#getPlanner} instead.
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
    public RelTraitSet getEmptyTraitSet() {
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
                ( cluster, relOptSchema, rootSchema ) -> {
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
            for ( RelTraitDef def : this.traitDefs ) {
                planner.addRelTraitDef( def );
            }
        }
    }


    @Override
    public SqlNode parse( final Reader reader ) throws SqlParseException {
        switch ( state ) {
            case STATE_0_CLOSED:
            case STATE_1_RESET:
                ready();
        }
        ensure( State.STATE_2_READY );
        SqlParser parser = SqlParser.create( reader, parserConfig );
        SqlNode sqlNode = parser.parseStmt();
        state = State.STATE_3_PARSED;
        return sqlNode;
    }


    @Override
    public SqlNode validate( SqlNode sqlNode ) throws ValidationException {
        ensure( State.STATE_3_PARSED );
        final SqlConformance conformance = conformance();
        final PolyphenyDbCatalogReader catalogReader = createCatalogReader();
        this.validator = new PolyphenyDbSqlValidator( operatorTable, catalogReader, typeFactory, conformance );
        this.validator.setIdentifierExpansion( true );
        try {
            validatedSqlNode = validator.validate( sqlNode );
        } catch ( RuntimeException e ) {
            throw new ValidationException( e );
        }
        state = State.STATE_4_VALIDATED;
        return validatedSqlNode;
    }


    private SqlConformance conformance() {
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
    public Pair<SqlNode, RelDataType> validateAndGetType( SqlNode sqlNode ) throws ValidationException {
        final SqlNode validatedNode = this.validate( sqlNode );
        final RelDataType type = this.validator.getValidatedNodeType( validatedNode );
        return Pair.of( validatedNode, type );
    }


    @Override
    public RelRoot rel( SqlNode sql ) throws RelConversionException {
        ensure( State.STATE_4_VALIDATED );
        assert validatedSqlNode != null;
        final RexBuilder rexBuilder = createRexBuilder();
        final RelOptCluster cluster = RelOptCluster.create( planner, rexBuilder );
        final SqlToRelConverter.Config config =
                SqlToRelConverter.configBuilder()
                        .withConfig( sqlToRelConverterConfig )
                        .withTrimUnusedFields( false )
                        .withConvertTableAccess( false )
                        .build();
        final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter( this, validator, createCatalogReader(), cluster, convertletTable, config );
        root = sqlToRelConverter.convertQuery( validatedSqlNode, false, true );
        root = root.withRel( sqlToRelConverter.flattenTypes( root.rel, true ) );
        final RelBuilder relBuilder = config.getRelBuilderFactory().create( cluster, null );
        root = root.withRel( RelDecorrelator.decorrelateQuery( root.rel, relBuilder ) );
        state = State.STATE_5_CONVERTED;
        return root;
    }


    @Override
    public RelRoot expandView( RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath ) {
        if ( planner == null ) {
            ready();
        }
        SqlParser parser = SqlParser.create( queryString, parserConfig );
        SqlNode sqlNode;
        try {
            sqlNode = parser.parseQuery();
        } catch ( SqlParseException e ) {
            throw new RuntimeException( "parse failed", e );
        }

        final SqlConformance conformance = conformance();
        final PolyphenyDbCatalogReader catalogReader = createCatalogReader().withSchemaPath( schemaPath );
        final SqlValidator validator = new PolyphenyDbSqlValidator( operatorTable, catalogReader, typeFactory, conformance );
        validator.setIdentifierExpansion( true );

        final RexBuilder rexBuilder = createRexBuilder();
        final RelOptCluster cluster = RelOptCluster.create( planner, rexBuilder );
        final SqlToRelConverter.Config config =
                SqlToRelConverter
                        .configBuilder()
                        .withConfig( sqlToRelConverterConfig )
                        .withTrimUnusedFields( false )
                        .withConvertTableAccess( false )
                        .build();
        final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter( this, validator, catalogReader, cluster, convertletTable, config );

        final RelRoot root = sqlToRelConverter.convertQuery( sqlNode, true, false );
        final RelRoot root2 = root.withRel( sqlToRelConverter.flattenTypes( root.rel, true ) );
        final RelBuilder relBuilder = config.getRelBuilderFactory().create( cluster, null );
        return root2.withRel( RelDecorrelator.decorrelateQuery( root.rel, relBuilder ) );
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
    public RelNode transform( int ruleSetIndex, RelTraitSet requiredOutputTraits, RelNode rel ) throws RelConversionException {
        ensure( State.STATE_5_CONVERTED );
        rel.getCluster().setMetadataProvider(
                new CachingRelMetadataProvider(
                        rel.getCluster().getMetadataProvider(),
                        rel.getCluster().getPlanner() ) );
        Program program = programs.get( ruleSetIndex );
        return program.run( planner, rel, requiredOutputTraits );
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

