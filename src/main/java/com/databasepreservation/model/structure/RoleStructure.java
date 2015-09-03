package com.databasepreservation.model.structure;

/**
 * @author Miguel Coutada
 */

public class RoleStructure {

        private String name;

        private String admin;

        private String description;

        /**
         *
         */
        public RoleStructure() {

        }

        public String getName() {
                return name;
        }

        public void setName(String name) {
                this.name = name;
        }

        public String getAdmin() {
                return admin;
        }

        public void setAdmin(String admin) {
                this.admin = admin;
        }

        public String getDescription() {
                return description;
        }

        public void setDescription(String description) {
                this.description = description;
        }

        @Override public String toString() {
                StringBuilder builder = new StringBuilder();
                builder.append("RoleStructure [name=");
                builder.append(name);
                builder.append(", admin=");
                builder.append(admin);
                builder.append(", description=");
                builder.append(description);
                builder.append("]");
                return builder.toString();
        }

        @Override public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + ((admin == null) ? 0 : admin.hashCode());
                result = prime * result + ((description == null) ? 0 : description.hashCode());
                result = prime * result + ((name == null) ? 0 : name.hashCode());
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
                RoleStructure other = (RoleStructure) obj;
                if (admin == null) {
                        if (other.admin != null) {
                                return false;
                        }
                } else if (!admin.equals(other.admin)) {
                        return false;
                }
                if (description == null) {
                        if (other.description != null) {
                                return false;
                        }
                } else if (!description.equals(other.description)) {
                        return false;
                }
                if (name == null) {
                        if (other.name != null) {
                                return false;
                        }
                } else if (!name.equals(other.name)) {
                        return false;
                }
                return true;
        }
}
