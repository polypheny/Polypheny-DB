/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.schema;


import ch.unibas.dmi.dbis.polyphenydb.materialize.Lattice;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.util.NameMap;
import ch.unibas.dmi.dbis.polyphenydb.util.NameMultimap;
import ch.unibas.dmi.dbis.polyphenydb.util.NameSet;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.tree.Expression;


/**
 * Schema.
 *
 * Wrapper around user-defined schema used internally.
 */
public abstract class AbstractPolyphenyDbSchema implements PolyphenyDbSchema {

    @Getter
    private final AbstractPolyphenyDbSchema parent;
    @Getter
    @Setter
    public Schema schema;
    @Getter
    public final String name;
    /**
     * Tables explicitly defined in this schema. Does not include tables in {@link #schema}.
     */
    protected final NameMap<TableEntry> tableMap;
    protected final NameMultimap<FunctionEntry> functionMap;
    protected final NameMap<TypeEntry> typeMap;
    protected final NameMap<LatticeEntry> latticeMap;
    protected final NameSet functionNames;
    protected final NameMap<FunctionEntry> nullaryFunctionMap;
    protected final NameMap<PolyphenyDbSchema> subSchemaMap;
    private List<? extends List<String>> path;


    protected AbstractPolyphenyDbSchema( AbstractPolyphenyDbSchema parent, Schema schema, String name, NameMap<PolyphenyDbSchema> subSchemaMap, NameMap<TableEntry> tableMap, NameMap<LatticeEntry> latticeMap, NameMap<TypeEntry> typeMap,
            NameMultimap<FunctionEntry> functionMap, NameSet functionNames, NameMap<FunctionEntry> nullaryFunctionMap, List<? extends List<String>> path ) {
        this.parent = parent;
        this.schema = schema;
        this.name = name;
        if ( tableMap == null ) {
            this.tableMap = new NameMap<>();
        } else {
            this.tableMap = Objects.requireNonNull( tableMap );
        }
        if ( latticeMap == null ) {
            this.latticeMap = new NameMap<>();
        } else {
            this.latticeMap = Objects.requireNonNull( latticeMap );
        }
        if ( subSchemaMap == null ) {
            this.subSchemaMap = new NameMap<>();
        } else {
            this.subSchemaMap = Objects.requireNonNull( subSchemaMap );
        }
        if ( functionMap == null ) {
            this.functionMap = new NameMultimap<>();
            this.functionNames = new NameSet();
            this.nullaryFunctionMap = new NameMap<>();
        } else {
            // If you specify functionMap, you must also specify functionNames and nullaryFunctionMap.
            this.functionMap = Objects.requireNonNull( functionMap );
            this.functionNames = Objects.requireNonNull( functionNames );
            this.nullaryFunctionMap = Objects.requireNonNull( nullaryFunctionMap );
        }
        if ( typeMap == null ) {
            this.typeMap = new NameMap<>();
        } else {
            this.typeMap = Objects.requireNonNull( typeMap );
        }
        this.path = path;
    }


    /**
     * Creates a root schema.
     *
     * @param cache If true create {@link CachingPolyphenyDbSchema}; if false create {@link SimplePolyphenyDbSchema}
     */
    public static PolyphenyDbSchema createRootSchema( boolean cache ) {
        return createRootSchema( cache, "" );
    }


    /**
     * Creates a root schema.
     *
     * @param cache If true create {@link CachingPolyphenyDbSchema}; if false create {@link SimplePolyphenyDbSchema}
     * @param name Schema name
     */
    public static PolyphenyDbSchema createRootSchema( boolean cache, String name ) {
        PolyphenyDbSchema rootSchema;
        final Schema schema = new RootSchema();
        if ( cache ) {
            rootSchema = new CachingPolyphenyDbSchema( null, schema, name );
        } else {
            rootSchema = new SimplePolyphenyDbSchema( null, schema, name );
        }
        return rootSchema;
    }


    /**
     * Returns a sub-schema with a given name that is defined implicitly (that is, by the underlying {@link Schema} object, not explicitly by a call to {@link #add(String, Schema)}), or null.
     */
    protected abstract PolyphenyDbSchema getImplicitSubSchema( String schemaName, boolean caseSensitive );

    /**
     * Returns a table with a given name that is defined implicitly (that is, by the underlying {@link Schema} object, not explicitly by a call to {@link #add(String, Table)}), or null.
     */
    protected abstract TableEntry getImplicitTable( String tableName, boolean caseSensitive );

    /**
     * Returns a type with a given name that is defined implicitly (that is, by the underlying {@link Schema} object, not explicitly by a call to {@link #add(String, RelProtoDataType)}), or null.
     */
    protected abstract TypeEntry getImplicitType( String name, boolean caseSensitive );

    /**
     * Returns table function with a given name and zero arguments that is defined implicitly (that is, by the underlying {@link Schema} object, not explicitly by a call to {@link #add(String, Function)}), or null.
     */
    protected abstract TableEntry getImplicitTableBasedOnNullaryFunction( String tableName, boolean caseSensitive );

    /**
     * Adds implicit sub-schemas to a builder.
     */
    protected abstract void addImplicitSubSchemaToBuilder( ImmutableSortedMap.Builder<String, PolyphenyDbSchema> builder );

    /**
     * Adds implicit tables to a builder.
     */
    protected abstract void addImplicitTableToBuilder( ImmutableSortedSet.Builder<String> builder );

    /**
     * Adds implicit functions to a builder.
     */
    protected abstract void addImplicitFunctionsToBuilder( ImmutableList.Builder<Function> builder, String name, boolean caseSensitive );

    /**
     * Adds implicit function names to a builder.
     */
    protected abstract void addImplicitFuncNamesToBuilder( ImmutableSortedSet.Builder<String> builder );

    /**
     * Adds implicit type names to a builder.
     */
    protected abstract void addImplicitTypeNamesToBuilder( ImmutableSortedSet.Builder<String> builder );

    /**
     * Adds implicit table functions to a builder.
     */
    protected abstract void addImplicitTablesBasedOnNullaryFunctionsToBuilder( ImmutableSortedMap.Builder<String, Table> builder );

    /**
     * Returns a snapshot representation of this PolyphenyDbSchema.
     */
    protected abstract PolyphenyDbSchema snapshot( AbstractPolyphenyDbSchema parent, SchemaVersion version );

    protected abstract boolean isCacheEnabled();


    /**
     * Creates a TableEntryImpl with no SQLs.
     */
    protected TableEntryImpl tableEntry( String name, Table table ) {
        return new TableEntryImpl( this, name, table, ImmutableList.of() );
    }


    /**
     * Creates a TableEntryImpl with no SQLs.
     */
    protected TypeEntryImpl typeEntry( String name, RelProtoDataType relProtoDataType ) {
        return new TypeEntryImpl( this, name, relProtoDataType );
    }


    /**
     * Defines a table within this schema.
     */
    @Override
    public TableEntry add( String tableName, Table table ) {
        return add( tableName, table, ImmutableList.of() );
    }


    /**
     * Defines a table within this schema.
     */
    @Override
    public TableEntry add( String tableName, Table table, ImmutableList<String> sqls ) {
        final TableEntryImpl entry = new TableEntryImpl( this, tableName, table, sqls );
        tableMap.put( tableName, entry );
        return entry;
    }


    /**
     * Defines a type within this schema.
     */
    @Override
    public TypeEntry add( String name, RelProtoDataType type ) {
        final TypeEntry entry = new TypeEntryImpl( this, name, type );
        typeMap.put( name, entry );
        return entry;
    }


    private FunctionEntry add( String name, Function function ) {
        final FunctionEntryImpl entry = new FunctionEntryImpl( this, name, function );
        functionMap.put( name, entry );
        functionNames.add( name );
        if ( function.getParameters().isEmpty() ) {
            nullaryFunctionMap.put( name, entry );
        }
        return entry;
    }


    private LatticeEntry add( String name, Lattice lattice ) {
        if ( latticeMap.containsKey( name, false ) ) {
            throw new RuntimeException( "Duplicate lattice '" + name + "'" );
        }
        final LatticeEntryImpl entry = new LatticeEntryImpl( this, name, lattice );
        latticeMap.put( name, entry );
        return entry;
    }


    @Override
    public AbstractPolyphenyDbSchema root() {
        for ( AbstractPolyphenyDbSchema schema = this; ; ) {
            if ( schema.parent == null ) {
                return schema;
            }
            schema = schema.parent;
        }
    }


    /**
     * Returns whether this is a root schema.
     */
    @Override
    public boolean isRoot() {
        return parent == null;
    }


    /**
     * Returns the path of an object in this schema.
     */
    @Override
    public List<String> path( String name ) {
        final List<String> list = new ArrayList<>();
        if ( name != null ) {
            list.add( name );
        }
        for ( AbstractPolyphenyDbSchema s = this; s != null; s = s.parent ) {
            if ( s.parent != null || !s.name.equals( "" ) ) {
                // Omit the root schema's name from the path if it's the empty string, which it usually is.
                list.add( s.name );
            }
        }
        return ImmutableList.copyOf( Lists.reverse( list ) );
    }


    @Override
    public final PolyphenyDbSchema getSubSchema( String schemaName, boolean caseSensitive ) {
        // Check explicit schemas.
        //noinspection LoopStatementThatDoesntLoop
        for ( Map.Entry<String, PolyphenyDbSchema> entry : subSchemaMap.range( schemaName, caseSensitive ).entrySet() ) {
            return entry.getValue();
        }
        return getImplicitSubSchema( schemaName, caseSensitive );
    }


    /**
     * Returns a table that materializes the given SQL statement.
     */
    @Override
    public final TableEntry getTableBySql( String sql ) {
        for ( TableEntry tableEntry : tableMap.map().values() ) {
            if ( tableEntry.sqls.contains( sql ) ) {
                return tableEntry;
            }
        }
        return null;
    }


    /**
     * Returns a table with the given name. Does not look for views.
     */
    @Override
    public final TableEntry getTable( String tableName, boolean caseSensitive ) {
        // Check explicit tables.
        //noinspection LoopStatementThatDoesntLoop
        for ( Map.Entry<String, TableEntry> entry : tableMap.range( tableName, caseSensitive ).entrySet() ) {
            return entry.getValue();
        }
        return getImplicitTable( tableName, caseSensitive );
    }


    @Override
    public SchemaPlus plus() {
        return new SchemaPlusImpl();
    }


    /**
     * Returns the default path resolving functions from this schema.
     *
     * The path consists is a list of lists of strings.
     * Each list of strings represents the path of a schema from the root schema. For example, [[], [foo], [foo, bar, baz]] represents three schemas: the root schema "/" (level 0), "/foo" (level 1) and "/foo/bar/baz" (level 3).
     *
     * @return Path of this schema; never null, may be empty
     */
    @Override
    public List<? extends List<String>> getPath() {
        if ( path != null ) {
            return path;
        }
        // Return a path consisting of just this schema.
        return ImmutableList.of( path( null ) );
    }


    /**
     * Returns a collection of sub-schemas, both explicit (defined using {@link #add(String, Schema)})
     * and implicit (defined using {@link Schema#getSubSchemaNames()} and {@link Schema#getSubSchema(String)}).
     */
    @Override
    public final NavigableMap<String, PolyphenyDbSchema> getSubSchemaMap() {
        // Build a map of implicit sub-schemas first, then explicit sub-schemas.
        // If there are implicit and explicit with the same name, explicit wins.
        final ImmutableSortedMap.Builder<String, PolyphenyDbSchema> builder = new ImmutableSortedMap.Builder<>( NameSet.COMPARATOR );
        builder.putAll( subSchemaMap.map() );
        addImplicitSubSchemaToBuilder( builder );
        return builder.build();
    }


    /**
     * Returns a collection of lattices.
     *
     * All are explicit (defined using {@link #add(String, Lattice)}).
     */
    @Override
    public NavigableMap<String, LatticeEntry> getLatticeMap() {
        return ImmutableSortedMap.copyOf( latticeMap.map() );
    }


    /**
     * Returns the set of all table names. Includes implicit and explicit tables and functions with zero parameters.
     */
    @Override
    public final NavigableSet<String> getTableNames() {
        final ImmutableSortedSet.Builder<String> builder = new ImmutableSortedSet.Builder<>( NameSet.COMPARATOR );
        // Add explicit tables, case-sensitive.
        builder.addAll( tableMap.map().keySet() );
        // Add implicit tables, case-sensitive.
        addImplicitTableToBuilder( builder );
        return builder.build();
    }


    /**
     * Returns the set of all types names.
     */
    @Override
    public final NavigableSet<String> getTypeNames() {
        final ImmutableSortedSet.Builder<String> builder = new ImmutableSortedSet.Builder<>( NameSet.COMPARATOR );
        // Add explicit types.
        builder.addAll( typeMap.map().keySet() );
        // Add implicit types.
        addImplicitTypeNamesToBuilder( builder );
        return builder.build();
    }


    /**
     * Returns a type, explicit and implicit, with a given name. Never null.
     */
    @Override
    public final TypeEntry getType( String name, boolean caseSensitive ) {
        for ( Map.Entry<String, TypeEntry> entry : typeMap.range( name, caseSensitive ).entrySet() ) {
            return entry.getValue();
        }
        return getImplicitType( name, caseSensitive );
    }


    /**
     * Returns a collection of all functions, explicit and implicit, with a given name. Never null.
     */
    @Override
    public final Collection<Function> getFunctions( String name, boolean caseSensitive ) {
        final ImmutableList.Builder<Function> builder = ImmutableList.builder();
        // Add explicit functions.
        for ( FunctionEntry functionEntry : Pair.right( functionMap.range( name, caseSensitive ) ) ) {
            builder.add( functionEntry.getFunction() );
        }
        // Add implicit functions.
        addImplicitFunctionsToBuilder( builder, name, caseSensitive );
        return builder.build();
    }


    /**
     * Returns the list of function names in this schema, both implicit and explicit, never null.
     */
    @Override
    public final NavigableSet<String> getFunctionNames() {
        final ImmutableSortedSet.Builder<String> builder = new ImmutableSortedSet.Builder<>( NameSet.COMPARATOR );
        // Add explicit functions, case-sensitive.
        builder.addAll( functionMap.map().keySet() );
        // Add implicit functions, case-sensitive.
        addImplicitFuncNamesToBuilder( builder );
        return builder.build();
    }


    /**
     * Returns tables derived from explicit and implicit functions that take zero parameters.
     */
    @Override
    public final NavigableMap<String, Table> getTablesBasedOnNullaryFunctions() {
        ImmutableSortedMap.Builder<String, Table> builder = new ImmutableSortedMap.Builder<>( NameSet.COMPARATOR );
        for ( Map.Entry<String, FunctionEntry> entry : nullaryFunctionMap.map().entrySet() ) {
            final Function function = entry.getValue().getFunction();
            if ( function instanceof TableMacro ) {
                assert function.getParameters().isEmpty();
                final Table table = ((TableMacro) function).apply( ImmutableList.of() );
                builder.put( entry.getKey(), table );
            }
        }
        // add tables derived from implicit functions
        addImplicitTablesBasedOnNullaryFunctionsToBuilder( builder );
        return builder.build();
    }


    /**
     * Returns a tables derived from explicit and implicit functions
     * that take zero parameters.
     */
    @Override
    public final TableEntry getTableBasedOnNullaryFunction( String tableName, boolean caseSensitive ) {
        for ( Map.Entry<String, FunctionEntry> entry : nullaryFunctionMap.range( tableName, caseSensitive ).entrySet() ) {
            final Function function = entry.getValue().getFunction();
            if ( function instanceof TableMacro ) {
                assert function.getParameters().isEmpty();
                final Table table = ((TableMacro) function).apply( ImmutableList.of() );
                return tableEntry( tableName, table );
            }
        }
        return getImplicitTableBasedOnNullaryFunction( tableName, caseSensitive );
    }


    /**
     * Creates a snapshot of this PolyphenyDbSchema as of the specified time. All explicit objects in this PolyphenyDbSchema will be copied into the snapshot PolyphenyDbSchema, while the contents of the snapshot of the underlying schema
     * should not change as specified in {@link Schema#snapshot(SchemaVersion)}. Snapshots of explicit sub schemas will be created and copied recursively.
     *
     * Currently, to accommodate the requirement of creating tables on the fly for materializations, the snapshot will still use the same table map and lattice map as in the original PolyphenyDbSchema instead of making copies.
     *
     * @param version The current schema version
     * @return the schema snapshot.
     */
    @Override
    public PolyphenyDbSchema createSnapshot( SchemaVersion version ) {
        Preconditions.checkArgument( this.isRoot(), "must be root schema" );
        return snapshot( null, version );
    }


    /**
     * Returns a subset of a map whose keys match the given string case-insensitively.
     *
     * @deprecated use NameMap
     */
    @Deprecated // to be removed before 2.0
    protected static <V> NavigableMap<String, V> find( NavigableMap<String, V> map, String s ) {
        return NameMap.immutableCopyOf( map ).range( s, false );
    }


    /**
     * Returns a subset of a set whose values match the given string case-insensitively.
     *
     * @deprecated use NameSet
     */
    @Deprecated // to be removed before 2.0
    protected static Iterable<String> find( NavigableSet<String> set, String name ) {
        return NameSet.immutableCopyOf( set ).range( name, false );
    }


    @Override
    @Experimental
    public boolean removeSubSchema( String name ) {
        return subSchemaMap.remove( name ) != null;
    }


    @Override
    @Experimental
    public boolean removeTable( String name ) {
        return tableMap.remove( name ) != null;
    }


    @Override
    @Experimental
    public boolean removeFunction( String name ) {
        final FunctionEntry remove = nullaryFunctionMap.remove( name );
        if ( remove == null ) {
            return false;
        }
        functionMap.remove( name, remove );
        return true;
    }


    @Override
    @Experimental
    public boolean removeType( String name ) {
        return typeMap.remove( name ) != null;
    }


    /**
     * Implementation of {@link SchemaPlus} based on a {@link AbstractPolyphenyDbSchema}.
     */
    private class SchemaPlusImpl implements SchemaPlus {

        @Override
        public AbstractPolyphenyDbSchema polyphenyDbSchema() {
            return AbstractPolyphenyDbSchema.this;
        }


        @Override
        public SchemaPlus getParentSchema() {
            return parent == null ? null : parent.plus();
        }


        @Override
        public String getName() {
            return AbstractPolyphenyDbSchema.this.getName();
        }


        @Override
        public boolean isMutable() {
            return schema.isMutable();
        }


        @Override
        public void setCacheEnabled( boolean cache ) {
            AbstractPolyphenyDbSchema.this.setCache( cache );
        }


        @Override
        public boolean isCacheEnabled() {
            return AbstractPolyphenyDbSchema.this.isCacheEnabled();
        }


        @Override
        public Schema snapshot( SchemaVersion version ) {
            throw new UnsupportedOperationException();
        }


        @Override
        public Expression getExpression( SchemaPlus parentSchema, String name ) {
            return schema.getExpression( parentSchema, name );
        }


        @Override
        public Table getTable( String name ) {
            final TableEntry entry = AbstractPolyphenyDbSchema.this.getTable( name, true );
            return entry == null ? null : entry.getTable();
        }


        @Override
        public NavigableSet<String> getTableNames() {
            return AbstractPolyphenyDbSchema.this.getTableNames();
        }


        @Override
        public RelProtoDataType getType( String name ) {
            final TypeEntry entry = AbstractPolyphenyDbSchema.this.getType( name, true );
            return entry == null ? null : entry.getType();
        }


        @Override
        public Set<String> getTypeNames() {
            return AbstractPolyphenyDbSchema.this.getTypeNames();
        }


        @Override
        public Collection<Function> getFunctions( String name ) {
            return AbstractPolyphenyDbSchema.this.getFunctions( name, true );
        }


        @Override
        public NavigableSet<String> getFunctionNames() {
            return AbstractPolyphenyDbSchema.this.getFunctionNames();
        }


        @Override
        public SchemaPlus getSubSchema( String name ) {
            final PolyphenyDbSchema subSchema = AbstractPolyphenyDbSchema.this.getSubSchema( name, true );
            return subSchema == null ? null : subSchema.plus();
        }


        @Override
        public Set<String> getSubSchemaNames() {
            return AbstractPolyphenyDbSchema.this.getSubSchemaMap().keySet();
        }


        @Override
        public SchemaPlus add( String name, Schema schema ) {
            final PolyphenyDbSchema polyphenyDbSchema = AbstractPolyphenyDbSchema.this.add( name, schema );
            return polyphenyDbSchema.plus();
        }


        @Override
        public <T> T unwrap( Class<T> clazz ) {
            if ( clazz.isInstance( this ) ) {
                return clazz.cast( this );
            }
            if ( clazz.isInstance( AbstractPolyphenyDbSchema.this ) ) {
                return clazz.cast( AbstractPolyphenyDbSchema.this );
            }
            if ( clazz.isInstance( AbstractPolyphenyDbSchema.this.schema ) ) {
                return clazz.cast( AbstractPolyphenyDbSchema.this.schema );
            }
            throw new ClassCastException( "not a " + clazz );
        }


        @Override
        public void setPath( ImmutableList<ImmutableList<String>> path ) {
            AbstractPolyphenyDbSchema.this.path = path;
        }


        @Override
        public void add( String name, Table table ) {
            AbstractPolyphenyDbSchema.this.add( name, table );
        }


        @Override
        public void add( String name, Function function ) {
            AbstractPolyphenyDbSchema.this.add( name, function );
        }


        @Override
        public void add( String name, RelProtoDataType type ) {
            AbstractPolyphenyDbSchema.this.add( name, type );
        }


        @Override
        public void add( String name, Lattice lattice ) {
            AbstractPolyphenyDbSchema.this.add( name, lattice );
        }
    }


}

