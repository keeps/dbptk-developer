package com.databasepreservation.model.structure;

/**
 * @author Miguel Coutada
 */

public class PrivilegeStructure {

        private String type;

        private String object;

        private String grantor;

        private String grantee;

        private String option;

        private String description;

        public PrivilegeStructure() {
        }

        public String getType() {
                return type;
        }

        public void setType(String type) {
                this.type = type;
        }

        public String getObject() {
                return object;
        }

        public void setObject(String object) {
                this.object = object;
        }

        public String getGrantor() {
                return grantor;
        }

        public void setGrantor(String grantor) {
                this.grantor = grantor;
        }

        public String getGrantee() {
                return grantee;
        }

        public void setGrantee(String grantee) {
                this.grantee = grantee;
        }

        public String getOption() {
                return option;
        }

        public void setOption(String option) {
                this.option = option;
        }

        public String getDescription() {
                return description;
        }

        public void setDescription(String description) {
                this.description = description;
        }

        @Override public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + ((description == null) ? 0 : description.hashCode());
                result = prime * result + ((grantee == null) ? 0 : grantee.hashCode());
                result = prime * result + ((grantor == null) ? 0 : grantor.hashCode());
                result = prime * result + ((object == null) ? 0 : object.hashCode());
                result = prime * result + ((option == null) ? 0 : option.hashCode());
                result = prime * result + ((type == null) ? 0 : type.hashCode());
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
                PrivilegeStructure other = (PrivilegeStructure) obj;
                if (description == null) {
                        if (other.description != null) {
                                return false;
                        }
                } else if (!description.equals(other.description)) {
                        return false;
                }
                if (grantee == null) {
                        if (other.grantee != null) {
                                return false;
                        }
                } else if (!grantee.equals(other.grantee)) {
                        return false;
                }
                if (grantor == null) {
                        if (other.grantor != null) {
                                return false;
                        }
                } else if (!grantor.equals(other.grantor)) {
                        return false;
                }
                if (object == null) {
                        if (other.object != null) {
                                return false;
                        }
                } else if (!object.equals(other.object)) {
                        return false;
                }
                if (option == null) {
                        if (other.option != null) {
                                return false;
                        }
                } else if (!option.equals(other.option)) {
                        return false;
                }
                if (type == null) {
                        if (other.type != null) {
                                return false;
                        }
                } else if (!type.equals(other.type)) {
                        return false;
                }
                return true;
        }
}
