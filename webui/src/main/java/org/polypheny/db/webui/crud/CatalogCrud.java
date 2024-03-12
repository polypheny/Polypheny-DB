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

package org.polypheny.db.webui.crud;

import io.javalin.http.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.models.AlgNodeModel;
import org.polypheny.db.webui.models.AssetsModel;
import org.polypheny.db.webui.models.SidebarElement;
import org.polypheny.db.webui.models.catalog.SnapshotModel;
import org.polypheny.db.webui.models.catalog.requests.NamespaceRequest;
import org.polypheny.db.webui.models.catalog.schema.NamespaceModel;
import org.polypheny.db.webui.models.requests.SchemaTreeRequest;
import org.reflections.Reflections;

@Slf4j
public class CatalogCrud {

    private static Crud crud;


    public CatalogCrud( Crud crud ) {
        CatalogCrud.crud = crud;
    }


    public void getNamespaces( Context context ) {
        NamespaceRequest request = context.bodyAsClass( NamespaceRequest.class );
        List<NamespaceModel> namespaces = Catalog.getInstance()
                .getSnapshot()
                .getNamespaces( request.pattern != null ? Pattern.of( request.pattern ) : null )
                .stream().map( NamespaceModel::from ).toList();
        context.json( namespaces );
    }


    public void getTypeNamespaces( final Context ctx ) {
        ctx.json( Catalog.snapshot()
                .getNamespaces( null )
                .stream()
                .collect( Collectors.toMap( LogicalNamespace::getName, LogicalNamespace::getDataModel ) ) );
    }


    public void getSchemaTree( final Context ctx ) {
        SchemaTreeRequest request = ctx.bodyAsClass( SchemaTreeRequest.class );
        List<SidebarElement> result = new ArrayList<>();

        if ( request.depth < 1 ) {
            log.error( "Trying to fetch a schemaTree with depth < 1" );
            ctx.json( new ArrayList<>() );
        }

        List<LogicalNamespace> namespaces = Catalog.snapshot().getNamespaces( null );
        // remove unwanted namespaces
        namespaces = namespaces.stream().filter( s -> request.dataModels.contains( s.dataModel ) ).toList();
        for ( LogicalNamespace namespace : namespaces ) {
            SidebarElement schemaTree = new SidebarElement( namespace.name, namespace.name, namespace.dataModel, "", getIconName( namespace.dataModel ) );

            if ( request.depth > 1 ) {
                switch ( namespace.dataModel ) {
                    case RELATIONAL:
                        attachTreeElements( namespace, request, schemaTree );
                        break;
                    case DOCUMENT:
                        attachDocumentTreeElements( namespace, request, schemaTree );
                        break;
                    case GRAPH:
                        schemaTree.setRouterLink( request.routerLinkRoot + "/" + namespace.name );
                        break;
                }
            }

            result.add( schemaTree );
        }

        ctx.json( result );
    }


    private void attachDocumentTreeElements( LogicalNamespace namespace, SchemaTreeRequest request, SidebarElement schemaTree ) {
        List<SidebarElement> collectionTree = new ArrayList<>();
        List<LogicalCollection> collections = Catalog.snapshot().doc().getCollections( namespace.id, null );
        for ( LogicalCollection collection : collections ) {
            SidebarElement tableElement = attachCollectionElement( namespace, request, collection );

            collectionTree.add( tableElement );
        }

        if ( request.showTable ) {
            schemaTree.addChild( new SidebarElement( namespace.name + ".tables", "tables", namespace.dataModel, request.routerLinkRoot, "fa fa-table" ).addChildren( collectionTree ).setRouterLink( "" ) );
        } else {
            schemaTree.addChildren( collectionTree ).setRouterLink( "" );
        }
    }


    @NotNull
    private static SidebarElement attachCollectionElement( LogicalNamespace namespace, SchemaTreeRequest request, LogicalCollection collection ) {
        String icon = "cui-description";
        if ( collection.entityType == EntityType.SOURCE ) {
            icon = "fa fa-plug";
        } else if ( collection.entityType == EntityType.VIEW ) {
            icon = "icon-eye";
        }

        SidebarElement tableElement = new SidebarElement( namespace.name + "." + collection.name, collection.name, namespace.dataModel, request.routerLinkRoot, icon );

        if ( request.views ) {
            if ( collection.entityType == EntityType.ENTITY || collection.entityType == EntityType.SOURCE ) {
                tableElement.setTableType( "TABLE" );
            } else if ( collection.entityType == EntityType.VIEW ) {
                tableElement.setTableType( "VIEW" );
            } else if ( collection.entityType == EntityType.MATERIALIZED_VIEW ) {
                tableElement.setTableType( "MATERIALIZED" );
            }
        }
        return tableElement;
    }


    private void attachTreeElements( LogicalNamespace namespace, SchemaTreeRequest request, SidebarElement schemaTree ) {
        List<SidebarElement> collectionTree = new ArrayList<>();
        List<LogicalTable> tables = Catalog.snapshot().rel().getTables( namespace.id, null );
        for ( LogicalTable table : tables ) {
            String icon = "fa fa-table";
            if ( table.entityType == EntityType.SOURCE ) {
                icon = "fa fa-plug";
            } else if ( table.entityType == EntityType.VIEW ) {
                icon = "icon-eye";
            }
            if ( table.entityType != EntityType.VIEW && namespace.dataModel == DataModel.DOCUMENT ) {
                icon = "cui-description";
            }

            SidebarElement tableElement = new SidebarElement( namespace.name + "." + table.name, table.name, namespace.dataModel, request.routerLinkRoot, icon );
            if ( request.depth > 2 ) {
                List<LogicalColumn> columns = Catalog.snapshot().rel().getColumns( table.id );
                for ( LogicalColumn column : columns ) {
                    tableElement.addChild( new SidebarElement( namespace.name + "." + table.name + "." + column.name, column.name, namespace.dataModel, request.routerLinkRoot, icon ).setCssClass( "sidebarColumn" ) );
                }
            }

            if ( request.views ) {
                if ( table.entityType == EntityType.ENTITY || table.entityType == EntityType.SOURCE ) {
                    tableElement.setTableType( "TABLE" );
                } else if ( table.entityType == EntityType.VIEW ) {
                    tableElement.setTableType( "VIEW" );
                } else if ( table.entityType == EntityType.MATERIALIZED_VIEW ) {
                    tableElement.setTableType( "MATERIALIZED" );
                }
            }

            collectionTree.add( tableElement );
        }

        if ( request.showTable ) {
            schemaTree.addChild( new SidebarElement( namespace.name + ".tables", "tables", namespace.dataModel, request.routerLinkRoot, "fa fa-table" ).addChildren( collectionTree ).setRouterLink( "" ) );
        } else {
            schemaTree.addChildren( collectionTree ).setRouterLink( "" );
        }
    }


    private String getIconName( DataModel dataModel ) {
        return switch ( dataModel ) {
            case RELATIONAL -> "cui-layers";
            case DOCUMENT -> "cui-folder";
            case GRAPH -> "cui-graph";
        };
    }


    public void getSnapshot( Context context ) {
        Long id = context.bodyAsClass( Long.class );

        Snapshot snapshot = Catalog.snapshot();
        if ( snapshot.id() > id ) {
            context.json( SnapshotModel.from( snapshot ) );
            return;
        }
        context.result( "" );
    }


    public void getCurrentSnapshot( Context context ) {
        context.json( Catalog.snapshot().id() );
    }


    public void getAssetsDefinition( Context context ) {
        context.json( new AssetsModel() );
    }


    public void getAlgebraNodes( Context context ) {
        Reflections reflections = new Reflections( "org.polypheny" );
        Map<String, List<AlgNodeModel>> nodes = Map.of(
                //"common", new ArrayList<>(),
                "relational", new ArrayList<>()
                //"document", new ArrayList<>(),
                /*"graph", new ArrayList<>()*/ );
        reflections.getSubTypesOf( AlgNode.class ).stream()
                .filter( c -> c.getSimpleName().startsWith( "Logical" ) ).forEach( c -> {
                    AlgNodeModel model = AlgNodeModel.from( c );
                    if ( nodes.containsKey( model.getModel() ) ) {
                        nodes.get( model.getModel() ).add( model );
                    }
                } );

        context.json( nodes );
    }

}
