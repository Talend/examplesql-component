package com.examplesql.talend.components.output.statement;

import com.examplesql.talend.components.output.Reject;
import com.examplesql.talend.components.service.ExampleSQLComponentService;
import org.talend.sdk.component.api.record.Record;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;

public interface QueryManager extends Serializable {

    List<Reject> execute(List<Record> records, ExampleSQLComponentService.DataSource dataSource) throws SQLException, IOException;
}
