package com.examplesql.talend.components.source;

import java.io.Serializable;

import com.examplesql.talend.components.dataset.SQLQueryDataset;
import com.examplesql.talend.components.dataset.TableNameDataset;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@GridLayout({
        // the generated layout put one configuration entry per line,
        // customize it as much as needed
        @GridLayout.Row({ "dataset" })
})
@Documentation("TODO fill the documentation for this configuration")
public class SQLQueryInputMapperConfiguration implements Serializable {
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private SQLQueryDataset dataset;

    public SQLQueryDataset getDataset() {
        return dataset;
    }

    public SQLQueryInputMapperConfiguration setDataset(SQLQueryDataset dataset) {
        this.dataset = dataset;
        return this;
    }
}