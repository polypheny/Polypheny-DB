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

package org.polypheny.db.monitoring.Ui;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.monitoring.persistence.MonitoringPersistentData;
import org.polypheny.db.monitoring.persistence.ReadOnlyMonitoringRepository;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class MonitoringServiceUiImpl implements MonitoringServiceUi {

    private InformationPage informationPage;
    private ReadOnlyMonitoringRepository repo;

    public MonitoringServiceUiImpl(ReadOnlyMonitoringRepository repo) {
        if (repo == null) {
            throw new IllegalArgumentException("repo parameter is null");
        }
        this.repo = repo;
    }

    @Override
    public void initializeInformationPage() {
        //Initialize Information Page
        informationPage = new InformationPage("Workload Monitoring CM");
        informationPage.fullWidth();
        InformationManager im = InformationManager.getInstance();
        im.addPage(informationPage);
    }

    @Override
    public <TPersistent extends MonitoringPersistentData> void registerPersistentClass(Class<TPersistent> registerClass) {
        String className = registerClass.getName();
        val informationGroup = new InformationGroup(informationPage, className);

        // TODO: see todo below
        val fieldAsString = Arrays.stream(registerClass.getDeclaredFields()).map(f -> f.getName()).filter(str -> str != "serialVersionUID").collect(Collectors.toList());
        val informationTable = new InformationTable(informationGroup, fieldAsString);

        informationGroup.setRefreshFunction(() -> this.updateQueueInformationTable(informationTable, registerClass));

        InformationManager im = InformationManager.getInstance();
        im.addGroup(informationGroup);
        im.registerInformation(informationTable);
    }

    private <TPersistent extends MonitoringPersistentData> void updateQueueInformationTable(InformationTable table, Class<TPersistent> registerClass) {
        List<TPersistent> elements = this.repo.GetAll(registerClass);
        table.reset();

        Field[] fields = registerClass.getDeclaredFields();
        Method[] methods = registerClass.getMethods();
        for (TPersistent element : elements) {
            List<String> row = new LinkedList<>();

            for (Field field : fields) {
                // TODO: get declared fields and fine corresponding Lombok getter to execute
                // Therefore, nothing need to be done for serialVersionID
                // and neither do we need to hacky set the setAccessible flag for the fields
                if (field.getName() == "serialVersionUID") {
                    continue;
                }

                try {
                    field.setAccessible(true);
                    val value = field.get(element);
                    row.add(value.toString());
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }

            table.addRow(row);
        }
    }
}
