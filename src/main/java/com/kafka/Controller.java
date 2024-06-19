package com.kafka;

import com.kafka.connector.CreateConnector;
import com.kafka.connector.DeleteConnector;
import com.kafka.connector.InjectSchema;
import com.kafka.connector.ListConnectors;
import com.kafka.service.StreamBreakaway;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import com.kafka.service.StreamAdd;
import com.kafka.service.StreamCondition;
import com.kafka.service.StreamDelete;

import java.util.List;

@RestController
@RequestMapping("/kafka/")
public class Controller {
    private final StreamAdd add;
    private final StreamDelete delete;
    private final StreamCondition filterCondition;
    private final StreamBreakaway breakaway;
    private final CreateConnector createConnector;
    private final DeleteConnector deleteConnector;
    private final ListConnectors listConnectors;
    private final InjectSchema injectSchema;

    @Autowired
    public Controller(StreamAdd add, StreamDelete delete, StreamCondition filterCondition,
                      StreamBreakaway breakaway, CreateConnector createConnector,
                      DeleteConnector deleteConnector, ListConnectors listConnectors,
                      InjectSchema injectSchema) {
        this.add = add;
        this.delete = delete;
        this.filterCondition = filterCondition;
        this.breakaway = breakaway;
        this.createConnector = createConnector;
        this.deleteConnector = deleteConnector;
        this.listConnectors = listConnectors;
        this.injectSchema = injectSchema;
    }

    @PostMapping("/column/{columnName}")
    public void addColumn(@PathVariable String columnName) {
        add.streamsAdd(columnName);
    }

    @DeleteMapping("/column/{columnName}")
    public void deleteColumn(@PathVariable String columnName) {
        delete.streamsDelete(columnName);
    }


    @PostMapping("/filter/{columnName}/{operator}/{condition}")
    public void filterColumn(@PathVariable String columnName, @PathVariable String operator, @PathVariable String condition) {
        filterCondition.streamsCondition(columnName, operator, condition);
    }

    @PostMapping("/breakaway/{seconds}")
    public void setBreakaway(@PathVariable int seconds) {
        breakaway.streamsBreakaway(seconds);
    }


    @PostMapping("/connectors/{connectorName}/{topic}")
    public void mkConnector(@PathVariable String connectorName, @PathVariable String topic) {
        String processedTopic = injectSchema.injecting(topic);
        createConnector.mkConnector(connectorName,processedTopic);
    }

    @DeleteMapping("/connectors/{connectorName}")
    public void deleteConnector(@PathVariable String connectorName) {
        deleteConnector.deleteConnector(connectorName);
    }

    @GetMapping("/connectors")
    public List<String> getConnectors() {
        return listConnectors.getConnectors();
    }
}
