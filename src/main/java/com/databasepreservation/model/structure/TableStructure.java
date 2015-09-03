/**
 *
 */
package com.databasepreservation.model.structure;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.utils.ListUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Luis Faria
 */
public class TableStructure {

        private String id;

        private String name;

        private String description;

        private List<ColumnStructure> columns;

        private PrimaryKey primaryKey;

        private List<ForeignKey> foreignKeys;

        private List<CandidateKey> candidateKeys;

        private List<CheckConstraint> checkConstraints;

        private List<Trigger> triggers;

        private long rows;

        private String schema;

        private long currentRow;

        private int index;

        /**
         * Empty table constructor. All fields are null except columns and foreign
         * keys, which are empty lists
         */
        public TableStructure() {
                id = null;
                name = null;
                description = null;
                columns = new ArrayList<ColumnStructure>();
                foreignKeys = new ArrayList<ForeignKey>();
                primaryKey = null;
                candidateKeys = new ArrayList<CandidateKey>();
                checkConstraints = new ArrayList<CheckConstraint>();
                triggers = new ArrayList<Trigger>();
                rows = -1;
                schema = null;

                currentRow = 1;
        }

        /**
         * TableStructure constructor
         *
         * @param id               the table id
         * @param name             the table name
         * @param description      the table description, optionally null
         * @param columns          the table columns
         * @param foreignKeys      foreign keys definition
         * @param primaryKey       primary key definition
         * @param candidateKeys    candidate keys definition
         * @param checkConstraints check constraints definition
         * @param triggers         triggers definition
         * @param rows             number of table rows
         */
        public TableStructure(String id, String name, String description, List<ColumnStructure> columns,
          List<ForeignKey> foreignKeys, PrimaryKey primaryKey, List<CandidateKey> candidateKeys,
          List<CheckConstraint> checkConstraints, List<Trigger> triggers, long rows) {
                isValidId(id);
                this.id = id;
                this.name = name;
                this.description = description;
                this.columns = columns;
                this.foreignKeys = foreignKeys;
                this.primaryKey = primaryKey;
                this.candidateKeys = candidateKeys;
                this.checkConstraints = checkConstraints;
                this.triggers = triggers;
                this.rows = rows;
        }

        /**
         * @return the table columns
         */
        public List<ColumnStructure> getColumns() {
                return columns;
        }

        /**
         * @param columns the table columns
         */
        public void setColumns(List<ColumnStructure> columns) {
                this.columns = columns;
        }

        /**
         * @return the table description, null when not defined
         */
        public String getDescription() {
                return description;
        }

        /**
         * @param description the table description, null when not defined
         */
        public void setDescription(String description) {
                this.description = description;
        }

        /**
         * @return the table unique id
         */
        public String getId() {
                return id;
        }

        /**
         * Sets a table id. Must be in the form of <schema>.
         * <table>
         *
         * @param id the table unique id
         * @throws ModuleException
         */
        public void setId(String id) {
                isValidId(id);
                this.id = id;
        }

        /**
         * @return the table name
         */
        public String getName() {
                return name;
        }

        /**
         * @param name the table name
         */
        public void setName(String name) {
                this.name = name;
        }

        /**
         * @return existing foreign keys
         */
        public List<ForeignKey> getForeignKeys() {
                return foreignKeys;
        }

        /**
         * @param foreignKeys existing foreign keys
         */
        public void setForeignKeys(List<ForeignKey> foreignKeys) {
                this.foreignKeys = foreignKeys;
        }

        /**
         * @return primary key, null if doesn't exist
         */
        public PrimaryKey getPrimaryKey() {
                return primaryKey;
        }

        /**
         * @param primaryKey primary key, null if doesn't exist
         */
        public void setPrimaryKey(PrimaryKey primaryKey) {
                this.primaryKey = primaryKey;
        }

        /**
         * @return the candidateKeys
         */
        public List<CandidateKey> getCandidateKeys() {
                return candidateKeys;
        }

        /**
         * @param candidateKeys the candidateKeys to set
         */
        public void setCandidateKeys(List<CandidateKey> candidateKeys) {
                this.candidateKeys = candidateKeys;
        }

        /**
         * @return the checkConstraints
         */
        public List<CheckConstraint> getCheckConstraints() {
                return checkConstraints;
        }

        /**
         * @param checkConstraints the checkConstraints to set
         */
        public void setCheckConstraints(List<CheckConstraint> checkConstraints) {
                this.checkConstraints = checkConstraints;
        }

        /**
         * @return the triggers
         */
        public List<Trigger> getTriggers() {
                return triggers;
        }

        /**
         * @param triggers the triggers to set
         */
        public void setTriggers(List<Trigger> triggers) {
                this.triggers = triggers;
        }

        /**
         * @return the rows
         */
        public long getRows() {
                return rows;
        }

        /**
         * @param rows the rows to set
         */
        public void setRows(long rows) {
                this.rows = rows;
        }

        public String getSchema() {
                return schema;
        }

        public void setSchema(SchemaStructure schema) {
                this.schema = schema.getName();
        }

        public void setSchema(String schema) {
                this.schema = schema;
        }

        protected void isValidId(String id) throws IllegalArgumentException {
                if (id.split("\\.").length < 2) {
                        throw new IllegalArgumentException("Table id must be in the form of <schema>.<table>");
                }
        }

        @Override public String toString() {
                StringBuilder builder = new StringBuilder();
                builder.append("\n\tTableStructure [id=");
                builder.append(id);
                builder.append(", name=");
                builder.append(name);
                builder.append(", description=");
                builder.append(description);
                builder.append(", index=");
                builder.append(index);
                builder.append(", columns=");
                builder.append(columns);
                builder.append(", primaryKey=");
                builder.append(primaryKey);
                builder.append(", foreignKeys=");
                builder.append(foreignKeys);
                builder.append(", candidateKeys=");
                builder.append(candidateKeys);
                builder.append(", checkConstraints=");
                builder.append(checkConstraints);
                builder.append(", triggers=");
                builder.append(triggers);
                builder.append(", rows=");
                builder.append(rows);
                builder.append("]");
                return builder.toString();
        }

        public long getCurrentRow() {
                return currentRow;
        }

        public void setCurrentRow(long currentRow) {
                this.currentRow = currentRow;
        }

        public long incrementCurrentRow() {
                currentRow = currentRow + 1;
                return currentRow;
        }

        @Override public boolean equals(Object obj) {
                if (this == obj) {
                        return true;
                }
                if (obj == null) {
                        return false;
                }
                if (getClass() != obj.getClass()) {
                        return false;
                }
                TableStructure other = (TableStructure) obj;
                if (candidateKeys == null) {
                        if (other.candidateKeys != null) {
                                return false;
                        }
                } else if (!ListUtils.equalsWithoutOrder(candidateKeys, other.candidateKeys)) {
                        return false;
                }
                if (index != other.index) {
                        return false;
                }
                if (checkConstraints == null) {
                        if (other.checkConstraints != null) {
                                return false;
                        }
                } else if (!ListUtils.equalsWithoutOrder(checkConstraints, other.checkConstraints)) {
                        return false;
                }
                if (columns == null) {
                        if (other.columns != null) {
                                return false;
                        }
                } else if (!ListUtils.equalsWithoutOrder(columns, other.columns)) {
                        return false;
                }
                if (currentRow != other.currentRow) {
                        return false;
                }
                if (description == null) {
                        if (other.description != null) {
                                return false;
                        }
                } else if (!description.equals(other.description)) {
                        return false;
                }
                if (foreignKeys == null) {
                        if (other.foreignKeys != null) {
                                return false;
                        }
                } else if (!ListUtils.equalsWithoutOrder(foreignKeys, other.foreignKeys)) {
                        return false;
                }
                if (id == null) {
                        if (other.id != null) {
                                return false;
                        }
                } else if (!id.equals(other.id)) {
                        return false;
                }
                if (name == null) {
                        if (other.name != null) {
                                return false;
                        }
                } else if (!name.equals(other.name)) {
                        return false;
                }
                if (primaryKey == null) {
                        if (other.primaryKey != null) {
                                return false;
                        }
                } else if (!primaryKey.equals(other.primaryKey)) {
                        return false;
                }
                if (rows != other.rows) {
                        return false;
                }
                if (schema == null) {
                        if (other.schema != null) {
                                return false;
                        }
                } else if (!schema.equals(other.schema)) {
                        return false;
                }
                if (triggers == null) {
                        if (other.triggers != null) {
                                return false;
                        }
                } else if (!ListUtils.equalsWithoutOrder(triggers, other.triggers)) {
                        return false;
                }
                return true;
        }

        @Override public int hashCode() {
                int result = id != null ? id.hashCode() : 0;
                result = 31 * result + (name != null ? name.hashCode() : 0);
                result = 31 * result + (description != null ? description.hashCode() : 0);
                result = 31 * result + (columns != null ? columns.hashCode() : 0);
                result = 31 * result + (primaryKey != null ? primaryKey.hashCode() : 0);
                result = 31 * result + (foreignKeys != null ? foreignKeys.hashCode() : 0);
                result = 31 * result + (candidateKeys != null ? candidateKeys.hashCode() : 0);
                result = 31 * result + (checkConstraints != null ? checkConstraints.hashCode() : 0);
                result = 31 * result + (triggers != null ? triggers.hashCode() : 0);
                result = 31 * result + (int) (rows ^ (rows >>> 32));
                result = 31 * result + (schema != null ? schema.hashCode() : 0);
                result = 31 * result + (int) (currentRow ^ (currentRow >>> 32));
                result = 31 * result + index;
                return result;
        }

        public int getIndex() {
                return index;
        }

        public void setIndex(int index) {
                this.index = index;
        }
}
