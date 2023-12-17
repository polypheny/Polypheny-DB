/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.backup.webui;

import io.javalin.http.Context;
import java.time.Instant;
import java.util.List;
import org.polypheny.db.backup.BackupManager;
import org.polypheny.db.backup.webui.models.ElementModel;
import org.polypheny.db.backup.webui.models.ManifestModel;
import org.polypheny.db.backup.webui.models.StatusModel;
import org.polypheny.db.backup.webui.models.StatusModel.Code;
import org.polypheny.db.type.entity.PolyTimestamp;
import org.polypheny.db.webui.ConfigService.HandlerType;
import org.polypheny.db.webui.HttpServer;

public class BackupCrud {

    private final BackupManager backupManager;


    public BackupCrud( BackupManager manager ) {
        this.backupManager = manager;

        registerBackupRoutes();
    }


    private void registerBackupRoutes() {
        HttpServer server = HttpServer.getInstance();
        final String PATH = "/backup/v1";

        server.addSerializedRoute( PATH + "/createBackup", this::createBackup, HandlerType.POST );

        server.addSerializedRoute( PATH + "/getCurrentStructure", this::getCurrentStructure, HandlerType.GET );

        server.addSerializedRoute( PATH + "/restoreBackup", this::restoreBackup, HandlerType.POST );

        server.addSerializedRoute( PATH + "/deleteBackup", this::deleteBackup, HandlerType.POST );

        server.addSerializedRoute( PATH + "/getBackups", this::getBackups, HandlerType.GET );
    }


    private void getBackups( Context context ) {
        //context.json( backupManager.getBackups().stream().map(b -> ManifestModel.from(Backup) ).collect( Collectors::toList)); todo ff enable after implementing mupltiple backups in BackupManager

        context.json( List.of(
                ManifestModel.getDummy(),
                new ManifestModel( 1, List.of(), new PolyTimestamp( Instant.now().toEpochMilli() ) ) ) );
    }


    private void deleteBackup( Context context ) {
        Long backupId = context.bodyAsClass( Long.class );

        // backupManager.deleteBackup( backupId ); todo ff enable after implementing in BackupManager
        context.json( new StatusModel( Code.SUCCESS, "Backup deleted" ) );
    }


    private void restoreBackup( Context context ) {
        ManifestModel manifestModel = context.bodyAsClass( ManifestModel.class );

        // backupManager.restoreBackup( backupId ); todo ff enable after implementing in BackupManager
        context.json( new StatusModel( Code.SUCCESS, "Backup restored" ) );
    }


    private void createBackup( Context context ) {
        List<ElementModel> elements = context.bodyAsClass( List.class );

        // backupManager.createBackup( elements ); todo ff enable after implementing in BackupManager
        context.json( new StatusModel( Code.SUCCESS, "Backup created" ) );
    }


    public void getCurrentStructure( Context context ) {
        // context.json( ElementModel.fromBackupObject( backupManager.getBackupInformationObject() ) ); todo ff enable after implementing in ElementModel
        context.json( ManifestModel.getDummy().getElements() );
    }


}
