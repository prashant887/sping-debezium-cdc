package com.example.spingdebeziumcdc.listener;
import io.debezium.config.Configuration;
import io.debezium.data.Envelope.Operation;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.source.SourceRecord;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.debezium.data.Envelope.FieldName.*;
import static java.util.stream.Collectors.toMap;
@Slf4j
@Component
public class CdcListener {

    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> debeziumEngine;
    private final Executor executor;


    public CdcListener(Configuration inventoryConnector) {
        this.executor = Executors.newSingleThreadExecutor();

        this.debeziumEngine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(inventoryConnector.asProperties())
                .notifying(this::handleChangeEvent).build();


    }
    private void handleChangeEvent(RecordChangeEvent<SourceRecord> recordChangeEvent) {

        SourceRecord sourceRecord = recordChangeEvent.record();

        log.info("Key = '"+sourceRecord.key()+"' value = '"+sourceRecord.value()+"'");
       // log.info(sourceRecord.kafkaPartition().toString());
       // log.info(sourceRecord.valueSchema().schema().name());

        Struct sourceRecordChangeValue= (Struct) sourceRecord.value();

        System.out.println(sourceRecordChangeValue);


        if (sourceRecordChangeValue != null) {
            String table=sourceRecordChangeValue.getStruct("source").getString("table");
            System.out.println("Table Name :"+table);

            Operation operation = Operation.forCode((String) sourceRecordChangeValue.get(OPERATION));

            System.out.println(operation);

            if(operation != Operation.READ) {
                String record = operation == Operation.DELETE ? BEFORE : AFTER; // Handling Update & Insert operations.

                System.out.println("record :"+record );
                System.out.println(" operation :"+operation.name());


                Struct struct = (Struct) sourceRecordChangeValue.get(record);

                System.out.println(struct);

                if (operation == Operation.UPDATE) {

                    Struct beforeStruct = (Struct) sourceRecordChangeValue.get(BEFORE);
                    Struct afterStruct = (Struct) sourceRecordChangeValue.get(AFTER);

                    System.out.println("Before :"+beforeStruct);
                    System.out.println("Before :"+beforeStruct.schema().fields().stream()
                            .map(Field::name)
                            .filter(fieldName -> beforeStruct.get(fieldName) != null)
                            .map(fieldName -> Pair.of(fieldName, beforeStruct.get(fieldName)))
                            .collect(toMap(Pair::getKey, Pair::getValue)));


                    System.out.println("After :"+afterStruct);
                    System.out.println("After :"+afterStruct.schema().fields().stream()
                            .map(Field::name)
                            .filter(fieldName -> afterStruct.get(fieldName) != null)
                            .map(fieldName -> Pair.of(fieldName, afterStruct.get(fieldName)))
                            .collect(toMap(Pair::getKey, Pair::getValue)));



                }

                Map<String, Object> payload = struct.schema().fields().stream()
                        .map(Field::name)
                        .filter(fieldName -> struct.get(fieldName) != null)
                        .map(fieldName -> Pair.of(fieldName, struct.get(fieldName)))
                        .collect(toMap(Pair::getKey, Pair::getValue));


                System.out.println("Payload :"+payload);


                }



        }


        }



        @PostConstruct
    private void start() {
        this.executor.execute(debeziumEngine);
    }

    @PreDestroy
    private void stop() throws Exception {
        if (this.debeziumEngine != null) {
            this.debeziumEngine.close();
        }
    }
}
