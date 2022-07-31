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

package org.polypheny.db.adapter.excel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.Adapter.AdapterSettingDirectory;
import org.polypheny.db.adapter.Adapter.AdapterSettingInteger;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.excel.ExcelTable.Flavor;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Source;
import org.polypheny.db.util.Sources;
@Slf4j
@AdapterProperties(
        name = "Excel",
        description = "An adapter for querying Excel files. The location of the directory containing the Excel files can be specified. Currently, this adapter only supports read operations.",
        usedModes = DeployMode.EMBEDDED)
@AdapterSettingDirectory(name = "directory", description = "You can upload one or multiple .xlsx.", position = 1)
@AdapterSettingInteger(name = "maxStringLength", defaultValue = 255, position = 2,
        description = "Which length (number of characters including whitespace) should be used for the varchar columns. Make sure this is equal or larger than the longest string in any of the columns.")
public class ExcelSource extends DataSource{
    private URL excelDir;
    private ExcelSchema currentSchema;
    private final int maxStringLength;
    private Map<String, List<ExportedColumn>> exportedColumnCache;
    public ExcelSource( int storeId, String uniqueName, Map<String, String> settings ) {

        super( storeId, uniqueName, settings, true );

        // Validate maxStringLength setting
        maxStringLength = Integer.parseInt( settings.get( "maxStringLength" ) );
        if ( maxStringLength < 1 ) {
            throw new RuntimeException( "Invalid value for maxStringLength: " + maxStringLength );
        }

        setExcelDir( settings );
        addInformationExportedColumns();
        enableInformationPage();
    }
    private void setExcelDir( Map<String, String> settings ) {
        String dir = settings.get( "directory" );
        if ( dir.startsWith( "classpath://" ) ) {
            excelDir = this.getClass().getClassLoader().getResource( dir.replace( "classpath://", "" ) + "/" );
        } else {
            try {
                excelDir = new File( dir ).toURI().toURL();
            } catch ( MalformedURLException e ) {
                throw new RuntimeException( e );
            }
        }
    }

    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        currentSchema = new ExcelSchema( excelDir, Flavor.SCANNABLE );
        //currentSchema = new ExcelSchema( excelDir);
    }


    @Override
    public Table createTableSchema( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement ) {
        return currentSchema.createExcelTable( catalogTable, columnPlacementsOnStore, this, partitionPlacement );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }


    @Override
    public void truncate( Context context, CatalogTable table ) {
        throw new RuntimeException( "Excel adapter does not support truncate" );
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
        final List<PolyType> types = new ArrayList<PolyType>();
        final List<String> names = new ArrayList<String>();
        if ( exportedColumnCache != null ) {
            return exportedColumnCache;
        }
        Map<String, List<ExportedColumn>> exportedColumnCache = new HashMap<>();
        Set<String> fileNames;
        if ( excelDir.getProtocol().equals( "jar" ) ) {
            List<CatalogColumnPlacement> ccps = Catalog
                    .getInstance()
                    .getColumnPlacementsOnAdapter( getAdapterId() );
            fileNames = new HashSet<>();
            for ( CatalogColumnPlacement ccp : ccps ) {
                fileNames.add( ccp.physicalSchemaName );
            }
        } else {
            File[] files = Sources.of( excelDir )
                    .file()
                    .listFiles( ( d, name ) -> name.endsWith( ".xlsx" ) || name.endsWith( ".xlsx.gz" ) ||name.endsWith( ".xls" ) || name.endsWith( ".xls.gz" ) );
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
            if ( physicalTableName.endsWith( ".xlsx" )){
                physicalTableName = physicalTableName
                        .substring( 0, physicalTableName.length() - ".xlsx".length() )
                        .trim()
                        .replaceAll( "[^a-z0-9_]+", "" );
            } else if (physicalTableName.endsWith( ".xls" ) ){
                physicalTableName = physicalTableName
                        .substring( 0, physicalTableName.length() - ".xls".length() )
                        .trim()
                        .replaceAll( "[^a-z0-9_]+", "" );
            }


            List<ExportedColumn> list = new ArrayList<>();
            int position = 1;
            try {
                Source source = Sources.of( new URL( excelDir, fileName ) );
                BufferedReader reader = new BufferedReader( source.reader() );
                File file = new File(source.path());   //creating a new file instance
                FileInputStream fs = new FileInputStream(file);

                Workbook workbook = WorkbookFactory.create(fs);
                Sheet sheet = workbook.getSheetAt(0);
                Iterator<Row> rowIterator = sheet.iterator();
                int columnCount=0;
                while (rowIterator.hasNext())
                {

                    Row row = rowIterator.next();
                    //For each row, iterate through all the columns
                    Iterator<Cell> cellIterator = row.cellIterator();

                    while (cellIterator.hasNext())
                    {
                        columnCount++;
                        Cell cell = cellIterator.next();
                        try{
                            names.add(cell.getStringCellValue());

                        }catch(Exception e){

                        }
                    }
                    break;

                }

                while (rowIterator.hasNext()) {
                    Row row = rowIterator.next();
                    Iterator<Cell> cellIterator = row.cellIterator();
                    while (cellIterator.hasNext()) {
                        Cell cell = cellIterator.next();
                        //ExcelFieldType fieldType = ExcelFieldType.of(cell);
                        PolyType type = PolyType.VARCHAR;
                        CellType cellType = cell.getCellType();
                        if (cellType == CellType.STRING) {
                            type = PolyType.VARCHAR;
                        } else if (cellType == CellType.NUMERIC) {
                            type = PolyType.DOUBLE;
                        } else if (cellType == CellType.BOOLEAN) {
                            type = PolyType.BOOLEAN;
                        } else if (cellType == CellType.BLANK) {
                            type = PolyType.VARCHAR;
                        }

                        types.add( type );

                    }
                    break;
                }

//                while (rows.hasNext()) {
//                    Row row = rows.next();
//                    Iterator<Cell> cellIterator = row.cellIterator();
//                    while (cellIterator.hasNext()) {
//                        Cell cell = cellIterator.next();
//                        names.add(cell.getStringCellValue());
//                    }
//                    break;
//                }
//                //types = new AlgDataType[names.size()];
//                //int rowCount = 1;
//                while (rows.hasNext()) {
//                    Row row = rows.next();
//                    Iterator<Cell> cellIterator = row.cellIterator();
//                    while (cellIterator.hasNext()) {
//                        Cell cell = cellIterator.next();
//                        ExcelFieldType fieldType = ExcelFieldType.of(cell);
//                        AlgDataType type;
//                        if (fieldType == null) {
//                            type = typeFactory.createJavaType(String.class);
//                        } else {
//                            type = fieldType.toType(typeFactory);
//                        }
//                        //names.add( name );
//                        types.add( type );
//                        if ( fieldTypes != null ) {
//                            fieldTypes.add( fieldType );
//                        }
////                    if (types[cell.getColumnIndex()] == null) {
////                        types[cell.getColumnIndex()] = type;
////                        // ExcelFieldTypes.add(fieldType);
////                    }
//                    }
//                    //rowCount++;
//                }


                for ( int i = 0; i < columnCount; i++ ) {
                    PolyType collectionsType = null;
                    Integer length = null;
                    Integer scale = null;
                    Integer dimension = null;
                    Integer cardinality = null;
                    String name = names.get( i );
                    PolyType type = types.get( i );
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
//                    switch ( typeStr.toLowerCase() ) {
//                        case "int":
//                            type = PolyType.INTEGER;
//                            break;
//                        case "string":
//                            type = PolyType.VARCHAR;
//                            length = maxStringLength;
//                            break;
//                        case "boolean":
//                            type = PolyType.BOOLEAN;
//                            break;
//                        case "long":
//                            type = PolyType.BIGINT;
//                            break;
//                        case "float":
//                            type = PolyType.REAL;
//                            break;
//                        case "double":
//                            type = PolyType.DOUBLE;
//                            break;
//                        case "date":
//                            type = PolyType.DATE;
//                            break;
//                        case "time":
//                            type = PolyType.TIME;
//                            length = 0;
//                            break;
//                        case "timestamp":
//                            type = PolyType.TIMESTAMP;
//                            length = 0;
//                            break;
//                        default:
//                            throw new RuntimeException( "Unknown type: " + typeStr.toLowerCase() );
//                    }

                }
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }

            exportedColumnCache.put( physicalTableName, list );
        }
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
}
