package com.databasepreservation.model.structure;

import com.databasepreservation.utils.ListUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Miguel Coutada
 */

public class ForeignKey {

        private String id;

        private String name;

        private String referencedSchema;

        private String referencedTable;

        private List<Reference> references;

        private String matchType;

        private String deleteAction;

        private String updateAction;

        private String description;

        /**
         *
         */
        public ForeignKey() {

        }

        /**
         * @param id
         * @param name
         * @param refTable
         * @param refCol
         */
        public ForeignKey(String id, String name, String refTable, String refCol) {
                this.id = id;
                this.name = name;
                this.referencedTable = refTable;
                Reference reference = new Reference();
                reference.setColumn(refCol);
                this.references = new ArrayList<Reference>();
        }

        /**
         * @param id
         * @param name
         * @param referencedSchema
         * @param referecendTable
         * @param reference
         * @param matchType
         * @param deleteAction
         * @param updateAction
         * @param description
         */
        public ForeignKey(String id, String name, String referencedSchema, String referencedTable,
          List<Reference> references, String matchType, String deleteAction, String updateAction, String description) {
                this.id = id;
                this.name = name;
                this.referencedSchema = referencedSchema;
                this.referencedTable = referencedTable;
                this.references = references;
                this.matchType = matchType;
                this.deleteAction = deleteAction;
                this.updateAction = updateAction;
                this.description = description;
        }

        /**
         * @return the unique id of the foreign key
         */
        public String getId() {
                return id;
        }

        /**
         * @param id the unique id for the foreign key
         */
        public void setId(String id) {
                this.id = id;
        }

        /**
         * @return the name
         */
        public String getName() {
                return name;
        }

        /**
         * @param name the name to set
         */
        public void setName(String name) {
                this.name = name;
        }

        /**
         * @return the referencedSchema
         */
        public String getReferencedSchema() {
                return referencedSchema;
        }

        /**
         * @param referencedSchema the referencedSchema to set
         */
        public void setReferencedSchema(String referencedSchema) {
                this.referencedSchema = referencedSchema;
        }

        /**
         * @return the referecendTable
         */
        public String getReferencedTable() {
                return referencedTable;
        }

        /**
         * @param referecendTable the referecendTable to set
         */
        public void setReferencedTable(String referencedTable) {
                this.referencedTable = referencedTable;
        }

        /**
         * @return the reference
         */
        public List<Reference> getReferences() {
                return references;
        }

        /**
         * @param reference the reference to set
         */
        public void setReferences(List<Reference> references) {
                this.references = references;
        }

        /**
         * @return the matchType
         */
        public String getMatchType() {
                return matchType;
        }

        /**
         * @param matchType the matchType to set
         */
        public void setMatchType(String matchType) {
                this.matchType = matchType;
        }

        /**
         * @return the deleteAction
         */
        public String getDeleteAction() {
                return deleteAction;
        }

        /**
         * @param deleteAction the deleteAction to set
         */
        public void setDeleteAction(String deleteAction) {
                this.deleteAction = deleteAction;
        }

        /**
         * @return the updateAction
         */
        public String getUpdateAction() {
                return updateAction;
        }

        /**
         * @param updateAction the updateAction to set
         */
        public void setUpdateAction(String updateAction) {
                this.updateAction = updateAction;
        }

        /**
         * @return the description
         */
        public String getDescription() {
                return description;
        }

        /**
         * @param description the description to set
         */
        public void setDescription(String description) {
                this.description = description;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override public String toString() {
                StringBuilder builder = new StringBuilder();
                builder.append("ForeignKey [name=");
                builder.append(name);
                builder.append(", id=");
                builder.append(id);
                builder.append(", referencedSchema=");
                builder.append(referencedSchema);
                builder.append(", referencedTable=");
                builder.append(referencedTable);
                builder.append(", reference=");
                builder.append(references);
                builder.append(", matchType=");
                builder.append(matchType);
                builder.append(", deleteAction=");
                builder.append(deleteAction);
                builder.append(", updateAction=");
                builder.append(updateAction);
                builder.append(", description=");
                builder.append(description);
                builder.append("]");
                return builder.toString();
        }

        @Override public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + ((deleteAction == null) ? 0 : deleteAction.hashCode());
                result = prime * result + ((description == null) ? 0 : description.hashCode());
                result = prime * result + ((id == null) ? 0 : id.hashCode());
                result = prime * result + ((matchType == null) ? 0 : matchType.hashCode());
                result = prime * result + ((name == null) ? 0 : name.hashCode());
                result = prime * result + ((referencedSchema == null) ? 0 : referencedSchema.hashCode());
                result = prime * result + ((referencedTable == null) ? 0 : referencedTable.hashCode());
                result = prime * result + ((references == null) ? 0 : references.hashCode());
                result = prime * result + ((updateAction == null) ? 0 : updateAction.hashCode());
                return result;
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
                ForeignKey other = (ForeignKey) obj;
                if (deleteAction == null) {
                        if (other.deleteAction != null) {
                                return false;
                        }
                } else if (!deleteAction.equals(other.deleteAction)) {
                        return false;
                }
                if (description == null) {
                        if (other.description != null) {
                                return false;
                        }
                } else if (!description.equals(other.description)) {
                        return false;
                }
                if (id == null) {
                        if (other.id != null) {
                                return false;
                        }
                } else if (!id.equals(other.id)) {
                        return false;
                }
                if (matchType == null) {
                        if (other.matchType != null) {
                                return false;
                        }
                } else if (!matchType.equals(other.matchType)) {
                        return false;
                }
                if (name == null) {
                        if (other.name != null) {
                                return false;
                        }
                } else if (!name.equals(other.name)) {
                        return false;
                }
                if (referencedSchema == null) {
                        if (other.referencedSchema != null) {
                                return false;
                        }
                } else if (!referencedSchema.equals(other.referencedSchema)) {
                        return false;
                }
                if (referencedTable == null) {
                        if (other.referencedTable != null) {
                                return false;
                        }
                } else if (!referencedTable.equals(other.referencedTable)) {
                        return false;
                }
                if (references == null) {
                        if (other.references != null) {
                                return false;
                        }
                } else if (!ListUtils.equalsWithoutOrder(references, other.references)) {
                        return false;
                }
                if (updateAction == null) {
                        if (other.updateAction != null) {
                                return false;
                        }
                } else if (!updateAction.equals(other.updateAction)) {
                        return false;
                }
                return true;
        }

}
