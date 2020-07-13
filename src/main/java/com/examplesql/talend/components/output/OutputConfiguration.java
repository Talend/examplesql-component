package com.examplesql.talend.components.output;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import com.examplesql.talend.components.dataset.TableNameDataset;
import com.examplesql.talend.components.service.I18nMessage;
import com.examplesql.talend.components.service.ExampleSQLComponentService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import static org.talend.sdk.component.api.configuration.condition.ActiveIf.EvaluationStrategy.CONTAINS;
import static org.talend.sdk.component.api.configuration.condition.ActiveIfs.Operator.OR;

@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "dataset" }),@GridLayout.Row({ "actionOnData" }),
        @GridLayout.Row({ "createTableIfNotExists" }),@GridLayout.Row({ "varcharLength" }),
        @GridLayout.Row({ "keys" }),@GridLayout.Row({ "ignoreUpdate" }),
        @GridLayout.Row({ "rewriteBatchedStatements" }),@GridLayout.Row({ "truncateTable" })
})
@Documentation("TODO fill the documentation for this configuration")
public class OutputConfiguration implements Serializable {
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private TableNameDataset dataset;

    @Option
    @Required
    @Suggestable(value = "actionsList", parameters = { "../dataset" })
    @Documentation("The action on data to be performed")
    private String actionOnData = "INSERT";

    @Option
    @Required
    @ActiveIf(target = "actionOnData", value = { "INSERT", "UPSERT", "BULK_LOAD" })
    @Documentation("Create table if don't exists")
    private boolean createTableIfNotExists = false;

    @Option
    @Required
    @ActiveIf(target = "../createTableIfNotExists", value = { "true" })
    @Documentation("The length of varchar types. This value will be used to create varchar columns in this table."
            + "\n-1 means that the max supported length of the targeted database will be used.")
    private int varcharLength = -1;

    @Option
    @ActiveIfs(operator = OR, value = { @ActiveIf(target = "../createTableIfNotExists", value = { "true" }),
            @ActiveIf(target = "../actionOnData", value = { "INSERT", "BULK_LOAD" }, negate = true)})
    @Suggestable(value = "suggestTableColumnNames", parameters = { "dataset" })
    @Documentation("List of columns to be used as keys for this operation")
    private List<String> keys = new ArrayList<>();

    @Option
    @Suggestable(value = "suggestTableColumnNames", parameters = { "../dataset" })
    @ActiveIf(target = "../actionOnData", value = { "UPDATE", "UPSERT" })
    @Documentation("List of columns to be ignored from update")
    private List<String> ignoreUpdate = new ArrayList<>();

    @Option
    @Documentation("Rewrite batched statements, to execute one statement per batch combining values in the sql query")
    private boolean rewriteBatchedStatements = true;

    @Option
    @Documentation("Truncate table before load")
    private boolean truncateTable = false;

    public TableNameDataset getDataset() {
        return dataset;
    }

    public OutputConfiguration setDataset(TableNameDataset dataset) {
        this.dataset = dataset;
        return this;
    }

    public String getActionOnData() { return this.actionOnData; }

    public OutputConfiguration setActionOnData(String actionOnData)
    {
        this.actionOnData = actionOnData;
        return this;
    }

    public boolean getCreateTableIfNotExists() { return this.createTableIfNotExists; }

    public OutputConfiguration setCreateTableIfNotExists(boolean createTableIfNotExists)
    {
        this.createTableIfNotExists = createTableIfNotExists;
        return this;
    }

    public int getVarcharLength() { return this.varcharLength; }

    public OutputConfiguration setVarcharLength(int varcharLength)
    {
        this.varcharLength = varcharLength;
        return this;
    }

    public List<String> getKeys() { return this.keys; }

    public OutputConfiguration setKeys(List<String> keys) {
        this.keys = keys;
        return this;
    }

    public List<String> getIgnoreUpdate() { return this.ignoreUpdate; }

    public OutputConfiguration setIgnoreUpdate(List<String> ignoreUpdate) {
        this.ignoreUpdate = ignoreUpdate;
        return this;
    }

    public boolean getRewriteBatchedStatements() { return this.rewriteBatchedStatements; }

    public OutputConfiguration setRewriteBatchedStatements(boolean rewriteBatchedStatements)
    {
        this.rewriteBatchedStatements = rewriteBatchedStatements;
        return this;
    }

    public boolean getTruncateTable() { return this.truncateTable; }

    public OutputConfiguration setTruncateTable(boolean truncateTable)
    {
        this.truncateTable = truncateTable;
        return this;
    }

    public ActionOnData isActionOnData() {
        if (actionOnData == null || actionOnData.isEmpty()) {
            throw new IllegalArgumentException("label on data is required");
        }

        return ActionOnData.getValueOf(actionOnData);
    }

    public enum ActionOnData {

        BULK_LOAD(I18nMessage::actionOnDataInsert, true),
        INSERT(I18nMessage::actionOnDataInsert, true),
        UPDATE(I18nMessage::actionOnDataUpdate, false),
        DELETE(I18nMessage::actionOnDataDelete, false),
        UPSERT(I18nMessage::actionOnDataUpsert, true);

        private ActionOnData(Function<I18nMessage, String> labelExtractor, boolean allowTableCreation)
        {
            this.labelExtractor = labelExtractor;
            this.allowTableCreation = allowTableCreation;
        }
        private final Function<I18nMessage, String> labelExtractor;


        private final boolean allowTableCreation;

        public String label(final I18nMessage messages) {
            return labelExtractor.apply(messages);
        }

        public boolean isAllowTableCreation() { return this.allowTableCreation; }

        public static ActionOnData getValueOf(String value)
        {
            for (ActionOnData e : values())
            {
                if (e.name().equals(value))
                    return e;
            }

            throw new IllegalArgumentException();
        }
    }

    public boolean isCreateTableIfNotExists() {
        return createTableIfNotExists && isActionOnData().isAllowTableCreation();
    }
}