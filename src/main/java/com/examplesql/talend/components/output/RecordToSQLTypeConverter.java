package com.examplesql.talend.components.output;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Optional;

public enum RecordToSQLTypeConverter {
    RECORD {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setObject(index, record.get(Record.class, entry.getName()).toString());
        }

    },
    ARRAY {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setArray(index, statement.getConnection().createArrayOf(entry.getName(),
                    record.getOptionalArray(Object.class, entry.getName()).orElseGet(ArrayList::new).toArray()));
        }

    },
    STRING {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            Optional<String> value = record.getOptionalString(entry.getName());
            if (value.isPresent()) {
                statement.setString(index, value.get());
            } else {
                statement.setNull(index, Types.VARCHAR);
            }
        }

    },
    BYTES {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setBytes(index, record.getOptionalBytes(entry.getName()).orElse(null));
        }

    },
    INT {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            if (record.getOptionalInt(entry.getName()).isPresent()) {
                statement.setInt(index, record.getInt(entry.getName()));
            } else {
                statement.setNull(index, Types.INTEGER);
            }
        }

    },
    LONG {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            if (record.getOptionalLong(entry.getName()).isPresent()) {
                statement.setLong(index, record.getLong(entry.getName()));
            } else {
                statement.setNull(index, Types.BIGINT);
            }
        }

    },
    FLOAT {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            if (record.getOptionalFloat(entry.getName()).isPresent()) {
                statement.setFloat(index, record.getFloat(entry.getName()));
            } else {
                statement.setNull(index, Types.FLOAT);
            }
        }

    },
    DOUBLE {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            if (record.getOptionalDouble(entry.getName()).isPresent()) {
                statement.setDouble(index, record.getDouble(entry.getName()));
            } else {
                statement.setNull(index, Types.DOUBLE);
            }
        }

    },
    BOOLEAN {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            if (record.getOptionalBoolean(entry.getName()).isPresent()) {
                statement.setBoolean(index, record.getBoolean(entry.getName()));
            } else {
                statement.setNull(index, Types.BOOLEAN);
            }
        }

    },
    DATETIME {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setTimestamp(index, record.getOptionalDateTime(entry.getName())
                    .map(d -> new Timestamp(d.toInstant().toEpochMilli())).orElse(null));
        }

    };

    public abstract void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry,
                                  final Record record) throws SQLException;

}

