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

package org.polypheny.db.tools;


import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataContext.SlimDataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.config.PolyphenyDbConnectionProperty;
import org.polypheny.db.core.OperatorTable;
import org.polypheny.db.jdbc.ContextImpl;
import org.polypheny.db.jdbc.JavaTypeFactoryImpl;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.NodeToRelConverter;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.languages.RexConvertletTable;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCostFactory;
import org.polypheny.db.plan.RelOptSchema;
import org.polypheny.db.plan.RelOptTable.ViewExpander;
import org.polypheny.db.plan.RelTraitDef;
import org.polypheny.db.prepare.PlannerImpl;
import org.polypheny.db.prepare.PolyphenyDbPrepareImpl;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rex.RexExecutor;
import org.polypheny.db.schema.AbstractPolyphenyDbSchema;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.util.Util;


/**
 * Tools for invoking Polypheny-DB functionality without initializing a container / server first.
 */
public class Frameworks {

    private Frameworks() {
    }


    /**
     * Creates a planner.
     *
     * @param config Planner configuration
     * @return Planner
     */
    public static Planner getPlanner( FrameworkConfig config ) {
        return new PlannerImpl( config );
    }


    /**
     * Piece of code to be run in a context where a planner is available. The planner is accessible from the {@code cluster}
     * parameter, as are several other useful objects.
     *
     * @param <R> result type
     */
    public interface PlannerAction<R> {

        R apply( RelOptCluster cluster, RelOptSchema relOptSchema, SchemaPlus rootSchema );

    }


    /**
     * Piece of code to be run in a context where a planner and statement are available. The planner is accessible from the
     * {@code cluster} parameter, as are several other useful objects. The connection and {@link DataContext} are accessible
     * from the statement.
     *
     * @param <R> result type
     */
    public abstract static class PrepareAction<R> {

        private final FrameworkConfig config;


        public PrepareAction( FrameworkConfig config ) {
            this.config = config;
        }


        public FrameworkConfig getConfig() {
            return config;
        }


        public abstract R apply(
                RelOptCluster cluster,
                RelOptSchema relOptSchema,
                SchemaPlus rootSchema );

    }


    /**
     * Initializes a container then calls user-specified code with a planner.
     *
     * @param action Callback containing user-specified code
     * @param config FrameworkConfig to use for planner action.
     * @return Return value from action
     */
    public static <R> R withPlanner( final PlannerAction<R> action, final FrameworkConfig config ) {
        return withPrepare(
                new Frameworks.PrepareAction<R>( config ) {
                    @Override
                    public R apply( RelOptCluster cluster, RelOptSchema relOptSchema, SchemaPlus rootSchema ) {
                        final PolyphenyDbSchema schema = PolyphenyDbSchema.from( Util.first( config.getDefaultSchema(), rootSchema ) );
                        return action.apply( cluster, relOptSchema, schema.root().plus() );
                    }
                } );
    }


    /**
     * Initializes a container then calls user-specified code with a planner.
     *
     * @param action Callback containing user-specified code
     * @return Return value from action
     */
    public static <R> R withPlanner( final PlannerAction<R> action ) {
        SchemaPlus rootSchema = Frameworks.createRootSchema( true );
        FrameworkConfig config = newConfigBuilder()
                .defaultSchema( rootSchema )
                .prepareContext( new ContextImpl(
                        PolyphenyDbSchema.from( rootSchema ),
                        new SlimDataContext() {
                            @Override
                            public JavaTypeFactory getTypeFactory() {
                                return new JavaTypeFactoryImpl();
                            }
                        },
                        "",
                        0,
                        0,
                        null ) )
                .build();
        return withPlanner( action, config );
    }


    /**
     * Initializes a container then calls user-specified code with a planner and statement.
     *
     * @param action Callback containing user-specified code
     * @return Return value from action
     */
    public static <R> R withPrepare( PrepareAction<R> action ) {
        try {
            final Properties info = new Properties();
            if ( action.config.getTypeSystem() != RelDataTypeSystem.DEFAULT ) {
                info.setProperty(
                        PolyphenyDbConnectionProperty.TYPE_SYSTEM.camelName(),
                        action.config.getTypeSystem().getClass().getName() );
            }

            return new PolyphenyDbPrepareImpl().perform( action );
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * Creates a root schema.
     *
     * @param cache Whether to create a caching schema.
     */
    public static SchemaPlus createRootSchema( boolean cache ) {
        return AbstractPolyphenyDbSchema.createRootSchema( "" ).plus();
    }


    /**
     * Creates a config builder with each setting initialized to its default value.
     */
    public static ConfigBuilder newConfigBuilder() {
        return new ConfigBuilder();
    }


    /**
     * Creates a config builder initializing each setting from an existing config.
     *
     * So, {@code newConfigBuilder(config).build()} will return a value equal to {@code config}.
     */
    public static ConfigBuilder newConfigBuilder( FrameworkConfig config ) {
        return new ConfigBuilder( config );
    }


    /**
     * A builder to help you build a {@link FrameworkConfig} using defaults where values aren't required.
     */
    public static class ConfigBuilder {

        private RexConvertletTable convertletTable;
        private OperatorTable operatorTable;
        private ImmutableList<Program> programs;
        private Context context;
        private ImmutableList<RelTraitDef> traitDefs;
        private ParserConfig parserConfig;
        private NodeToRelConverter.Config sqlToRelConverterConfig;
        private SchemaPlus defaultSchema;
        private RexExecutor executor;
        private RelOptCostFactory costFactory;
        private RelDataTypeSystem typeSystem;
        private ViewExpander viewExpander;
        private org.polypheny.db.jdbc.Context prepareContext;


        /**
         * Creates a ConfigBuilder, initializing to defaults.
         */
        public ConfigBuilder() {
            convertletTable = LanguageManager.getInstance().getStandardConvertlet();
            operatorTable = LanguageManager.getInstance().getStdOperatorTable();
            programs = ImmutableList.of();
            parserConfig = ParserConfig.DEFAULT;
            sqlToRelConverterConfig = NodeToRelConverter.Config.DEFAULT;
            typeSystem = RelDataTypeSystem.DEFAULT;
        }


        /**
         * Creates a ConfigBuilder, initializing from an existing config.
         */
        public ConfigBuilder( FrameworkConfig config ) {
            convertletTable = config.getConvertletTable();
            operatorTable = config.getOperatorTable();
            programs = config.getPrograms();
            context = config.getContext();
            traitDefs = config.getTraitDefs();
            parserConfig = config.getParserConfig();
            sqlToRelConverterConfig = config.getSqlToRelConverterConfig();
            defaultSchema = config.getDefaultSchema();
            executor = config.getExecutor();
            costFactory = config.getCostFactory();
            typeSystem = config.getTypeSystem();
            prepareContext = config.getPrepareContext();
        }


        public FrameworkConfig build() {
            return new StdFrameworkConfig(
                    context,
                    convertletTable,
                    operatorTable,
                    programs,
                    traitDefs,
                    parserConfig,
                    sqlToRelConverterConfig,
                    defaultSchema,
                    costFactory,
                    typeSystem,
                    executor,
                    viewExpander,
                    prepareContext );
        }


        public ConfigBuilder context( Context c ) {
            this.context = Objects.requireNonNull( c );
            return this;
        }


        public ConfigBuilder executor( RexExecutor executor ) {
            this.executor = Objects.requireNonNull( executor );
            return this;
        }


        public ConfigBuilder convertletTable( RexConvertletTable convertletTable ) {
            this.convertletTable = Objects.requireNonNull( convertletTable );
            return this;
        }


        public ConfigBuilder operatorTable( OperatorTable operatorTable ) {
            this.operatorTable = Objects.requireNonNull( operatorTable );
            return this;
        }


        public ConfigBuilder traitDefs( List<RelTraitDef> traitDefs ) {
            if ( traitDefs == null ) {
                this.traitDefs = null;
            } else {
                this.traitDefs = ImmutableList.copyOf( traitDefs );
            }
            return this;
        }


        public ConfigBuilder traitDefs( RelTraitDef... traitDefs ) {
            this.traitDefs = ImmutableList.copyOf( traitDefs );
            return this;
        }


        public ConfigBuilder parserConfig( ParserConfig parserConfig ) {
            this.parserConfig = Objects.requireNonNull( parserConfig );
            return this;
        }


        public ConfigBuilder sqlToRelConverterConfig( NodeToRelConverter.Config sqlToRelConverterConfig ) {
            this.sqlToRelConverterConfig = Objects.requireNonNull( sqlToRelConverterConfig );
            return this;
        }


        public ConfigBuilder defaultSchema( SchemaPlus defaultSchema ) {
            this.defaultSchema = defaultSchema;
            return this;
        }


        public ConfigBuilder costFactory( RelOptCostFactory costFactory ) {
            this.costFactory = costFactory;
            return this;
        }


        public ConfigBuilder ruleSets( RuleSet... ruleSets ) {
            return programs( Programs.listOf( ruleSets ) );
        }


        public ConfigBuilder ruleSets( List<RuleSet> ruleSets ) {
            return programs( Programs.listOf( Objects.requireNonNull( ruleSets ) ) );
        }


        public ConfigBuilder programs( List<Program> programs ) {
            this.programs = ImmutableList.copyOf( programs );
            return this;
        }


        public ConfigBuilder programs( Program... programs ) {
            this.programs = ImmutableList.copyOf( programs );
            return this;
        }


        public ConfigBuilder typeSystem( RelDataTypeSystem typeSystem ) {
            this.typeSystem = Objects.requireNonNull( typeSystem );
            return this;
        }


        public ConfigBuilder viewExpander( ViewExpander viewExpander ) {
            this.viewExpander = viewExpander;
            return this;
        }


        public ConfigBuilder prepareContext( org.polypheny.db.jdbc.Context prepareContext ) {
            this.prepareContext = prepareContext;
            return this;
        }

    }


    /**
     * An implementation of {@link FrameworkConfig} that uses standard Polypheny-DB classes to provide basic planner functionality.
     */
    public static class StdFrameworkConfig implements FrameworkConfig {

        private final Context context;
        private final RexConvertletTable convertletTable;
        private final OperatorTable operatorTable;
        private final ImmutableList<Program> programs;
        private final ImmutableList<RelTraitDef> traitDefs;
        private final ParserConfig parserConfig;
        private final NodeToRelConverter.Config sqlToRelConverterConfig;
        private final SchemaPlus defaultSchema;
        private final RelOptCostFactory costFactory;
        private final RelDataTypeSystem typeSystem;
        private final RexExecutor executor;
        private final ViewExpander viewExpander;
        private final org.polypheny.db.jdbc.Context prepareContext;


        public StdFrameworkConfig(
                Context context,
                RexConvertletTable convertletTable,
                OperatorTable operatorTable,
                ImmutableList<Program> programs,
                ImmutableList<RelTraitDef> traitDefs,
                ParserConfig parserConfig,
                NodeToRelConverter.Config sqlToRelConverterConfig,
                SchemaPlus defaultSchema,
                RelOptCostFactory costFactory,
                RelDataTypeSystem typeSystem,
                RexExecutor executor,
                ViewExpander viewExpander,
                org.polypheny.db.jdbc.Context prepareContext ) {
            this.context = context;
            this.convertletTable = convertletTable;
            this.operatorTable = operatorTable;
            this.programs = programs;
            this.traitDefs = traitDefs;
            this.parserConfig = parserConfig;
            this.sqlToRelConverterConfig = sqlToRelConverterConfig;
            this.defaultSchema = defaultSchema;
            this.costFactory = costFactory;
            this.typeSystem = typeSystem;
            this.executor = executor;
            this.viewExpander = viewExpander;
            this.prepareContext = prepareContext;
        }


        @Override
        public ParserConfig getParserConfig() {
            return parserConfig;
        }


        @Override
        public NodeToRelConverter.Config getSqlToRelConverterConfig() {
            return sqlToRelConverterConfig;
        }


        @Override
        public SchemaPlus getDefaultSchema() {
            return defaultSchema;
        }


        @Override
        public RexExecutor getExecutor() {
            return executor;
        }


        @Override
        public ImmutableList<Program> getPrograms() {
            return programs;
        }


        @Override
        public RelOptCostFactory getCostFactory() {
            return costFactory;
        }


        @Override
        public ImmutableList<RelTraitDef> getTraitDefs() {
            return traitDefs;
        }


        @Override
        public RexConvertletTable getConvertletTable() {
            return convertletTable;
        }


        @Override
        public Context getContext() {
            return context;
        }


        @Override
        public OperatorTable getOperatorTable() {
            return operatorTable;
        }


        @Override
        public RelDataTypeSystem getTypeSystem() {
            return typeSystem;
        }


        @Override
        public ViewExpander getViewExpander() {
            return viewExpander;
        }


        @Override
        public org.polypheny.db.jdbc.Context getPrepareContext() {
            return prepareContext;
        }

    }

}

