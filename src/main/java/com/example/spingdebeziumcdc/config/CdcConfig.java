package com.example.spingdebeziumcdc.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;

@Configuration
public class CdcConfig {

    @Value("${postgres.datasource.host}")
    private String inventoryDbHost;
    @Value("${postgres.datasource.port}")
    private String inventoryDbPort;
    @Value("${postgres.datasource.username}")
    private String inventoryDbUsername;
    @Value("${postgres.datasource.password}")
    private String inventoryDbPassword;
    @Value("${postgres.datasource.database}")
    private String inventoryDbName;

    @Bean
    public io.debezium.config.Configuration postgresConnector() throws IOException {
        File offsetStorageTempFile=File.createTempFile("offset_",".dat");
        File dbHistoryTempFile = File.createTempFile("dbhistory_", ".dat");
        System.out.printf("offsetStorageTempFile = %s \n dbHistoryTempFile=%s%n",offsetStorageTempFile,dbHistoryTempFile);

        io.debezium.config.Configuration configuration=
                io.debezium.config.Configuration.create()
                        .with("name", "postgres-postgresql-connector")
                        .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                        .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                        .with("offset.storage.file.filename", offsetStorageTempFile.getAbsolutePath())
                        .with("offset.flush.interval.ms", "60000")
                        .with("database.hostname", inventoryDbHost)
                        .with("database.port", inventoryDbPort)
                        .with("database.user", inventoryDbUsername)
                        .with("database.password", inventoryDbPassword)
                        .with("database.dbname", inventoryDbName)
      //                .with("database.include.list", inventoryDbName)
                        .with("include.schema.changes", "false")
                        .with("database.allowPublicKeyRetrieval", "true")
                        .with("database.server.id", "10181")
                        .with("database.server.name", "cdc-postgresql-server")
                        .with("topic.prefix", "debezium-cdc")
                        .with("plugin.name", "pgoutput")
                        .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
                        .with("database.history.file.filename", dbHistoryTempFile.getAbsolutePath())
                        //.with("table.exclude.list","batch_job_instance,batch_job_execution,batch_job_execution_params,batch_step_execution,batch_step_execution_context,batch_job_execution_context")
                        .with("schema.include.list","cdc")
                        .with("transforms.unwrap.type","io.debezium.transforms.ExtractNewRecordState")
                        //.with("table.whitelist", TABLES_TO_MONITOR)
                        .build();

        return configuration;

    }
}
