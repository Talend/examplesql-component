package com.examplesql.talend.components.output;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.util.List;
import java.util.Optional;

public enum RecordToBulkLoadConverter {

    RECORD {

        @Override
        public void setValue(final List<String> lines, final int index, final Schema.Entry entry, final Record record)
                throws IllegalArgumentException {
            throw new IllegalArgumentException("Record not supported for Bulk Loading");
        }

    },
    ARRAY {

        @Override
        public void setValue(final List<String> lines, final int index, final Schema.Entry entry, final Record record)
                throws IllegalArgumentException {
            throw new IllegalArgumentException("Array not supported for Bulk Loading");
        }

    },
    STRING {

        @Override
        public void setValue(final List<String> lines, final int index, final Schema.Entry entry, final Record record)
                throws IllegalArgumentException {
            Optional<String> value = record.getOptionalString(entry.getName());
            if (value.isPresent()) {
                lines.add("\""+value.get()+"\"");
            } else {
                lines.add("NULL");
            }
        }

    },
    BYTES {

        @Override
        public void setValue(final List<String> lines, final int index, final Schema.Entry entry, final Record record)
                throws IllegalArgumentException {
            throw new IllegalArgumentException("Byte Array not supported in Bulk Load");
        }

    },
    INT {

        @Override
        public void setValue(final List<String> lines, final int index, final Schema.Entry entry, final Record record)
                throws IllegalArgumentException {
            if (record.getOptionalInt(entry.getName()).isPresent()) {
                lines.add(String.valueOf(record.getInt(entry.getName())));
            } else {
                lines.add("NULL");
            }
        }

    },
    LONG {

        @Override
        public void setValue(final List<String> lines, final int index, final Schema.Entry entry, final Record record)
                throws IllegalArgumentException {
            if (record.getOptionalLong(entry.getName()).isPresent()) {
                lines.add(String.valueOf(record.getLong(entry.getName())));
            } else {
                lines.add("NULL");
            }
        }

    },
    FLOAT {

        @Override
        public void setValue(final List<String> lines, final int index, final Schema.Entry entry, final Record record)
                throws IllegalArgumentException {
            if (record.getOptionalFloat(entry.getName()).isPresent()) {
                lines.add(String.valueOf(record.getFloat(entry.getName())));
            } else {
                lines.add("NULL");
            }
        }

    },
    DOUBLE {

        @Override
        public void setValue(final List<String> lines, final int index, final Schema.Entry entry, final Record record)
                throws IllegalArgumentException {
            if (record.getOptionalDouble(entry.getName()).isPresent()) {
                lines.add(String.valueOf(record.getDouble(entry.getName())));
            } else {
                lines.add("NULL");
            }
        }

    },
    BOOLEAN {

        @Override
        public void setValue(final List<String> lines, final int index, final Schema.Entry entry, final Record record)
                throws IllegalArgumentException {
            if (record.getOptionalBoolean(entry.getName()).isPresent()) {
                lines.add(String.valueOf(record.getBoolean(entry.getName())));
            } else {
                lines.add("NULL");
            }
        }

    },
    DATETIME {

        @Override
        public void setValue(final List<String> lines, final int index, final Schema.Entry entry, final Record record)
                throws IllegalArgumentException {
            if (record.getOptionalDateTime(entry.getName()).isPresent())
                lines.add(String.valueOf(record.getOptionalDateTime(entry.getName())));
            else
                lines.add("NULL");

        }

    };

    public abstract void setValue(final List<String> lines, final int index, final Schema.Entry entry,
                                  final Record record) throws IllegalArgumentException;
}
