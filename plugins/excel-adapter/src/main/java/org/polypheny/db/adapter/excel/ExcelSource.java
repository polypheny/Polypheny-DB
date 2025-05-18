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

package org.polypheny.db.adapter.excel;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.polypheny.db.adapter.ConnectionMethod;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalScanDelegate;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingDirectory;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingList;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.adapter.excel.ExcelTable.Flavor;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schemaDiscovery.AbstractNode;
import org.polypheny.db.schemaDiscovery.AttributeNode;
import org.polypheny.db.schemaDiscovery.MetadataProvider;
import org.polypheny.db.schemaDiscovery.Node;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Source;
import org.polypheny.db.util.Sources;

@Slf4j
@AdapterProperties(
        name = "Excel",
        description = "An adapter for querying Excel files. The location of the directory containing the Excel files can be specified. Currently, this adapter only supports read operations.",
        usedModes = DeployMode.EMBEDDED,
        defaultMode = DeployMode.EMBEDDED)
@AdapterSettingList(name = "method", options = { "upload", "link" }, defaultValue = "upload", description = "If the supplied file(s) should be uploaded or a link to the local filesystem is used (sufficient permissions are required).", position = 1)
@AdapterSettingDirectory(subOf = "method_upload", name = "directory", description = "You can upload one or multiple .xlsx.", position = 1, defaultValue = "")
@AdapterSettingString(subOf = "method_link", defaultValue = ".", name = "directoryName", description = "You can select a path to a folder or specific .xls or .xlsx files.", position = 2)
@AdapterSettingString(name = "sheetName", description = "default to read the first sheet", defaultValue = "", required = false)
@AdapterSettingInteger(name = "maxStringLength", defaultValue = 255, position = 2,
        description = "Which length (number of characters including whitespace) should be used for the varchar columns. Make sure this is equal or larger than the longest string in any of the columns.")
public class ExcelSource extends DataSource<RelAdapterCatalog> implements MetadataProvider {

    public AbstractNode metadataRoot;
    private Map<String, List<Map<String, Object>>> previewByTable = new LinkedHashMap<>();

    @Delegate(excludes = Excludes.class)
    private final RelationalScanDelegate delegate;
    private final ConnectionMethod connectionMethod;
    private URL excelDir;
    @Getter
    private ExcelNamespace currentNamespace;
    private final int maxStringLength;
    private Map<String, List<ExportedColumn>> exportedColumnCache;
    public String sheetName;


    public ExcelSource( final long storeId, final String uniqueName, final Map<String, String> settings, final DeployMode mode ) {
        super( storeId, uniqueName, settings, mode, true, new RelAdapterCatalog( storeId ) );
        log.error( settings.get( "directory" ) );

        this.connectionMethod = settings.containsKey( "method" ) ? ConnectionMethod.from( settings.get( "method" ) ) : ConnectionMethod.UPLOAD;
        // Validate maxStringLength setting
        maxStringLength = Integer.parseInt( settings.get( "maxStringLength" ) );

        if ( maxStringLength < 1 ) {
            throw new GenericRuntimeException( "Invalid value for maxStringLength: " + maxStringLength );
        }
        this.sheetName = settings.get( "sheetName" );

        setExcelDir( settings );
        addInformationExportedColumns();
        enableInformationPage();

        this.delegate = new RelationalScanDelegate( this, adapterCatalog );
    }


    private void setExcelDir( Map<String, String> settings ) {
        String dir = settings.get( "directory" );
        log.error( "Directory kommt an als: " + settings.get( "directory" ) );

        if ( dir != null && dir.trim().startsWith( "[" ) ) {
            try {
                List<String> list = new ObjectMapper()
                        .readValue( dir, new TypeReference<List<String>>() {
                        } );
                dir = list.isEmpty() ? null : list.get( 0 );
                log.error( "Directory nach Parsing: " + dir );
            } catch ( IOException e ) {
                throw new GenericRuntimeException( "Cannot parse directory JSON", e );
            }
        }

        if ( connectionMethod == ConnectionMethod.LINK ) {
            dir = settings.get( "directoryName" );
            log.error( "DirectoryName kommt an als: " + settings.get( "directoryName" ) );
        }

        if ( dir == null ) {
            throw new GenericRuntimeException( "Directory must not be null" );
        }

        if ( dir.startsWith( "classpath://" ) ) {
            excelDir = this.getClass().getClassLoader().getResource( dir.replace( "classpath://", "" ) + "/" );
        } else {
            try {
                excelDir = new File( dir ).toURI().toURL();
            } catch ( MalformedURLException e ) {
                throw new GenericRuntimeException( e );
            }
        }
    }


    @Override
    public void updateNamespace( String name, long id ) {
        currentNamespace = new ExcelNamespace( id, adapterId, excelDir, Flavor.SCANNABLE, this.sheetName );
    }


    @Override
    public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
        PhysicalTable table = adapterCatalog.createTable(
                logical.table.getNamespaceName(),
                logical.table.name,
                logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c.name ) ),
                logical.table,
                logical.columns.stream().collect( Collectors.toMap( t -> t.id, t -> t ) ),
                logical.pkIds, allocation );

        ExcelTable physical = currentNamespace.createExcelTable( table, this );

        adapterCatalog.replacePhysical( physical );

        return List.of( physical );
    }


    @Override
    public void truncate( Context context, long allocId ) {
        throw new GenericRuntimeException( "Excel adapter does not support truncate" );
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "Excel Store does not support prepare()." );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        log.debug( "Excel Store does not support commit()." );
    }


    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "Excel Store does not support rollback()." );
    }


    @Override
    public void shutdown() {
        removeInformationPage();
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        if ( updatedSettings.contains( "directory" ) ) {
            setExcelDir( settings );
        }
    }


    @Override
    public Map<String, List<ExportedColumn>> getExportedColumns() {
        String currentSheetName;

        if ( connectionMethod == ConnectionMethod.UPLOAD && exportedColumnCache != null ) {
            // If we upload, file will not be changed, and we can cache the columns information, if "link" is used this is not advised
            return exportedColumnCache;
        }
        Map<String, List<ExportedColumn>> exportedColumnCache = new HashMap<>();
        Set<String> fileNames;
        if ( excelDir.getProtocol().equals( "jar" ) ) {
            List<AllocationEntity> placements = Catalog.snapshot().alloc().getEntitiesOnAdapter( getAdapterId() ).orElse( List.of() );
            fileNames = new HashSet<>();
            for ( AllocationEntity ccp : placements ) {
                fileNames.add( ccp.getNamespaceName() );
            }
        } else if ( Sources.of( excelDir ).file().isFile() ) {
            // single files
            fileNames = Set.of( excelDir.getPath() );
        } else {
            File[] files = Sources.of( excelDir )
                    .file()
                    .listFiles( ( d, name ) -> (name.endsWith( ".xlsx" ) || name.endsWith( ".xlsx.gz" ) || name.endsWith( ".xls" ) || name.endsWith( ".xls.gz" )) && !name.startsWith( "~$" ) );
            fileNames = Arrays.stream( files )
                    .sequential()
                    .map( File::getName )
                    .collect( Collectors.toSet() );
        }
        for ( String fileName : fileNames ) {
            // Compute physical table name
            String physicalTableName = fileName.toLowerCase();
            if ( physicalTableName.endsWith( ".gz" ) ) {
                physicalTableName = physicalTableName.substring( 0, physicalTableName.length() - ".gz".length() );
            }
            if ( physicalTableName.endsWith( ".xlsx" ) ) {
                physicalTableName = physicalTableName
                        .substring( 0, physicalTableName.length() - ".xlsx".length() )
                        .trim()
                        .replaceAll( "[^a-z0-9_]+", "" );
            } else if ( physicalTableName.endsWith( ".xls" ) ) {
                physicalTableName = physicalTableName
                        .substring( 0, physicalTableName.length() - ".xls".length() )
                        .trim()
                        .replaceAll( "[^a-z0-9_]+", "" );
            }

            List<ExportedColumn> list = new ArrayList<>();
            int position = 1;
            try {
                Source source = Sources.of( new URL( excelDir, fileName ) );
                File file = new File( source.path() );   //creating a new file instance
                FileInputStream fs = new FileInputStream( file );

                Workbook workbook = WorkbookFactory.create( fs );
                Sheet sheet;

                if ( this.sheetName.equals( "" ) ) {
                    sheet = workbook.getSheetAt( 0 );
                    currentSheetName = workbook.getSheetName( 0 );

                } else {
                    sheet = workbook.getSheet( this.sheetName );
                    currentSheetName = this.sheetName;
                }

                // Read first row to extract column attribute name and datatype
                for ( Row row : sheet ) {
                    // For each row, iterate through all the columns
                    Iterator<Cell> cellIterator = row.cellIterator();

                    while ( cellIterator.hasNext() ) {
                        Cell cell = cellIterator.next();
                        try {
                            String[] colSplit = cell.getStringCellValue().split( ":" );
                            String name = colSplit[0]
                                    .toLowerCase()
                                    .trim()
                                    .replaceAll( "[^a-z0-9_]+", "" );
                            String typeStr = "string";
                            if ( colSplit.length > 1 ) {
                                typeStr = colSplit[1].toLowerCase().trim();
                            }
                            PolyType collectionsType = null;
                            PolyType type;
                            Integer length = null;
                            Integer scale = null;
                            Integer dimension = null;
                            Integer cardinality = null;
                            switch ( typeStr.toLowerCase() ) {
                                case "int":
                                    type = PolyType.INTEGER;
                                    break;
                                case "string":
                                    type = PolyType.VARCHAR;
                                    length = maxStringLength;
                                    break;
                                case "boolean":
                                    type = PolyType.BOOLEAN;
                                    break;
                                case "long":
                                    type = PolyType.BIGINT;
                                    break;
                                case "float":
                                    type = PolyType.REAL;
                                    break;
                                case "double":
                                    type = PolyType.DOUBLE;
                                    break;
                                case "date":
                                    type = PolyType.DATE;
                                    break;
                                case "time":
                                    type = PolyType.TIME;
                                    length = 0;
                                    break;
                                case "timestamp":
                                    type = PolyType.TIMESTAMP;
                                    length = 0;
                                    break;
                                default:
                                    throw new GenericRuntimeException( "Unknown type: " + typeStr.toLowerCase() );
                            }

                            list.add( new ExportedColumn(
                                    name,
                                    type,
                                    collectionsType,
                                    length,
                                    scale,
                                    dimension,
                                    cardinality,
                                    false,
                                    fileName,
                                    physicalTableName,
                                    name,
                                    position,
                                    position == 1 ) ); // TODO

                            position++;
                        } catch ( Exception e ) {
                            throw new GenericRuntimeException( e );
                        }
                    }
                    break;
                }
            } catch ( IOException e ) {
                throw new GenericRuntimeException( e );
            }
            exportedColumnCache.put( physicalTableName + "_" + currentSheetName, list );
        }
        this.exportedColumnCache = exportedColumnCache;
        return exportedColumnCache;
    }


    private void addInformationExportedColumns() {
        for ( Map.Entry<String, List<ExportedColumn>> entry : getExportedColumns().entrySet() ) {
            InformationGroup group = new InformationGroup( informationPage, entry.getValue().get( 0 ).physicalSchemaName );
            informationGroups.add( group );

            InformationTable table = new InformationTable(
                    group,
                    Arrays.asList( "Position", "Column Name", "Type", "Nullable", "Filename", "Primary" ) );
            for ( ExportedColumn exportedColumn : entry.getValue() ) {
                table.addRow(
                        exportedColumn.physicalPosition,
                        exportedColumn.name,
                        exportedColumn.getDisplayType(),
                        exportedColumn.nullable ? "✔" : "",
                        exportedColumn.physicalSchemaName,
                        exportedColumn.primary ? "✔" : ""
                );
            }
            informationElements.add( table );
        }
    }


    @Override
    public AbstractNode fetchMetadataTree() {

        String filePath = "C:/Users/roman/Desktop/Mappe1.xlsx";
        String mappeName = "Workbook";

        AbstractNode root = new Node( "excel", mappeName );
        try ( Workbook wb = WorkbookFactory.create( new File( filePath ) ) ) {

            for ( Sheet sheet : wb ) {

                String sheetName = sheet.getSheetName();
                AbstractNode sheetNode = new Node( "sheet", sheetName );

                Row header = sheet.getRow( sheet.getFirstRowNum() );
                if ( header == null ) {
                    continue;
                }
                for ( int c = 0; c < header.getLastCellNum(); c++ ) {
                    Cell cell = header.getCell( c );
                    String colName = getCellValueAsString( cell, "COL_" + (c + 1) );

                    AbstractNode colNode = new AttributeNode( "column", colName );
                    colNode.addProperty( "type", inferType( sheet, c, header.getRowNum() + 1, 20 ) );
                    colNode.addProperty( "nullable", true );

                    sheetNode.addChild( colNode );
                }

                String fqName = mappeName + "." + sheetName;
                List<Map<String, Object>> rows = fetchPreview( null, fqName, 10 );
                this.previewByTable.put( fqName, rows );

                root.addChild( sheetNode );
            }

        } catch ( IOException e ) {
            throw new RuntimeException( "Failed to read Excel metadata: " + filePath, e );
        }

        this.metadataRoot = root;
        return metadataRoot;
    }


    private String inferType( Sheet sheet, int colIndex, int startRow, int maxRows ) {
        int checked = 0;
        for ( int r = startRow; r <= sheet.getLastRowNum() && checked < maxRows; r++ ) {
            Row row = sheet.getRow( r );
            if ( row == null ) {
                continue;
            }
            Cell cell = row.getCell( colIndex );
            if ( cell == null ) {
                continue;
            }

            switch ( cell.getCellType() ) {
                case NUMERIC:
                    if ( DateUtil.isCellDateFormatted( cell ) ) {
                        return "DATE";
                    }
                    return "DOUBLE";
                case STRING:
                    return "STRING";
                case BOOLEAN:
                    return "BOOLEAN";
                default:
                    continue;
            }
        }
        return "STRING";
    }


    private String getCellValueAsString( Cell cell, String fallback ) {
        if ( cell == null ) {
            return fallback;
        }
        try {
            return switch ( cell.getCellType() ) {
                case STRING -> cell.getStringCellValue();
                case NUMERIC -> String.valueOf( cell.getNumericCellValue() );
                case BOOLEAN -> String.valueOf( cell.getBooleanCellValue() );
                case FORMULA -> cell.getCellFormula();
                default -> fallback;
            };
        } catch ( Exception e ) {
            return fallback;
        }
    }


    @Override
    public List<Map<String, Object>> fetchPreview( Connection conn, String fqName, int limit ) {

        String[] parts = fqName.split( "\\.", 2 );
        String sheetName = parts.length == 2 ? parts[1] : parts[0];
        String filePath = "C:/Users/roman/Desktop/Mappe1.xlsx";

        List<Map<String, Object>> rows = new ArrayList<>();

        try ( Workbook wb = WorkbookFactory.create( new File( filePath ) ) ) {

            Sheet sheet = wb.getSheet( sheetName );
            if ( sheet == null ) {
                log.warn( "Sheet {} not found in {}", sheetName, filePath );
                return List.of();
            }

            Row header = sheet.getRow( sheet.getFirstRowNum() );
            if ( header == null ) {
                return List.of();
            }

            List<String> colNames = new ArrayList<>();
            for ( int c = 0; c < header.getLastCellNum(); c++ ) {
                colNames.add( getCellValueAsString( header.getCell( c ), "COL_" + (c + 1) ) );
            }

            int first = header.getRowNum() + 1;
            int last = Math.min( sheet.getLastRowNum(), first + limit - 1 );

            for ( int r = first; r <= last; r++ ) {
                Row dataRow = sheet.getRow( r );
                if ( dataRow == null ) {
                    continue;
                }

                Map<String, Object> map = new LinkedHashMap<>();
                for ( int c = 0; c < colNames.size(); c++ ) {
                    map.put( colNames.get( c ), extractCellValue( dataRow.getCell( c ) ) );
                }
                rows.add( map );
            }

        } catch ( IOException e ) {
            throw new RuntimeException( "Failed to read Excel preview: " + filePath, e );
        }

        return rows;
    }


    private Object extractCellValue( Cell cell ) {
        if ( cell == null ) {
            return null;
        }
        return switch ( cell.getCellType() ) {
            case STRING -> cell.getStringCellValue();
            case NUMERIC -> DateUtil.isCellDateFormatted( cell )
                    ? cell.getDateCellValue()
                    : cell.getNumericCellValue();
            case BOOLEAN -> cell.getBooleanCellValue();
            case FORMULA -> cell.getCellFormula();
            case BLANK -> null;
            default -> cell.toString();
        };
    }


    @Override
    public void markSelectedAttributes( List<String> selectedPaths ) {
        List<List<String>> attributePaths = new ArrayList<>();

        for ( String path : selectedPaths ) {
            String cleanPath = path.replaceFirst( " ?:.*$", "" ).trim();

            List<String> segments = Arrays.asList( cleanPath.split( "\\." ) );
            if ( !segments.isEmpty() && segments.get( 0 ).equals( metadataRoot.getName() ) ) {
                segments = segments.subList( 1, segments.size() );
            }

            attributePaths.add( segments );
        }

        for ( List<String> pathSegments : attributePaths ) {
            AbstractNode current = metadataRoot;

            for ( int i = 0; i < pathSegments.size(); i++ ) {
                String segment = pathSegments.get( i );

                if ( i == pathSegments.size() - 1 ) {
                    Optional<AbstractNode> attrNodeOpt = current.getChildren().stream()
                            .filter( c -> c instanceof AttributeNode && segment.equals( c.getName() ) )
                            .findFirst();

                    if ( attrNodeOpt.isPresent() ) {
                        ((AttributeNode) attrNodeOpt.get()).setSelected( true );
                        log.info( "✅ Attribut gesetzt: " + String.join( ".", pathSegments ) );
                    } else {
                        log.warn( "❌ Attribut nicht gefunden: " + String.join( ".", pathSegments ) );
                    }

                } else {
                    Optional<AbstractNode> childOpt = current.getChildren().stream()
                            .filter( c -> segment.equals( c.getName() ) )
                            .findFirst();

                    if ( childOpt.isPresent() ) {
                        current = childOpt.get();
                    } else {
                        log.warn( "❌ Segment nicht gefunden: " + segment + " im Pfad " + String.join( ".", pathSegments ) );
                        break;
                    }
                }
            }
        }

    }


    @Override
    public void printTree( AbstractNode node, int depth ) {
        if ( node == null ) {
            node = this.metadataRoot;
        }
        System.out.println( "  ".repeat( depth ) + node.getType() + ": " + node.getName() );
        for ( Map.Entry<String, Object> entry : node.getProperties().entrySet() ) {
            System.out.println( "  ".repeat( depth + 1 ) + "- " + entry.getKey() + ": " + entry.getValue() );
        }
        for ( AbstractNode child : node.getChildren() ) {
            printTree( child, depth + 1 );
        }

    }


    @Override
    public void setRoot( AbstractNode root ) {
        this.metadataRoot = root;
    }


    @Override
    public Object getPreview() {
        return this.previewByTable;
    }


    @SuppressWarnings("unused")
    private interface Excludes {

        void refreshTable( long allocId );

        void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation );

    }

}
