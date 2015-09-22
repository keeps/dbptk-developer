package com.databasepreservation.model.structure;

import com.databasepreservation.utils.ListUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Miguel Coutada
 */

public class SchemaStructure {

        private String name;

        private String description;

        private int index;

        private List<TableStructure> tables;

        private List<ViewStructure> views;

        private List<RoutineStructure> routines;

        /**
         * @param name
         * @param description
         * @param index
         * @param tables
         * @param views
         * @param routines
         */
        public SchemaStructure(String name, String description, int index, List<TableStructure> tables,
          List<ViewStructure> views, List<RoutineStructure> routines) {
                super();
                this.name = name;
                this.description = description;
                this.index = index;
                this.tables = tables;
                this.views = views;
                this.routines = routines;
        }

        public SchemaStructure() {
                tables = new ArrayList<TableStructure>();
                views = new ArrayList<ViewStructure>();
                routines = new ArrayList<RoutineStructure>();
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

        /**
         * @return the tables
         */
        public List<TableStructure> getTables() {
                return tables;
        }

        /**
         * @param tables the tables to set
         */
        public void setTables(List<TableStructure> tables) {
                this.tables = tables;
        }

        /**
         * @return the views
         */
        public List<ViewStructure> getViews() {
                return views;
        }

        /**
         * @param views the views to set
         */
        public void setViews(List<ViewStructure> views) {
                this.views = views;
        }

        /**
         * @return the routines
         */
        public List<RoutineStructure> getRoutines() {
                return routines;
        }

        /**
         * @param routines the routines to set
         */
        public void setRoutines(List<RoutineStructure> routines) {
                this.routines = routines;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override public String toString() {
                StringBuilder builder = new StringBuilder();
                builder.append("\n****** SCHEMA: " + name + " ******");
                builder.append("\n");
                builder.append("description=");
                builder.append(description);
                builder.append("\n");
                builder.append("index=");
                builder.append(index);
                builder.append("\n");
                builder.append("tables=");
                builder.append(tables);
                builder.append("\n");
                builder.append("views=");
                builder.append(views);
                builder.append("\n");
                builder.append("routines=");
                builder.append(routines);
                builder.append("\n");
                builder.append("****** END SCHEMA ******");
                builder.append("\n");
                return builder.toString();
        }

        @Override public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + ((description == null) ? 0 : description.hashCode());
                result = prime * result + index;
                result = prime * result + ((name == null) ? 0 : name.hashCode());
                result = prime * result + ((routines == null) ? 0 : routines.hashCode());
                result = prime * result + ((tables == null) ? 0 : tables.hashCode());
                result = prime * result + ((views == null) ? 0 : views.hashCode());
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
                SchemaStructure other = (SchemaStructure) obj;
                if (description == null) {
                        if (other.description != null) {
                                return false;
                        }
                } else if (!description.equals(other.description)) {
                        return false;
                }
                if (index != other.index) {
                        return false;
                }
                if (name == null) {
                        if (other.name != null) {
                                return false;
                        }
                } else if (!name.equals(other.name)) {
                        return false;
                }
                if (routines == null) {
                        if (other.routines != null) {
                                return false;
                        }
                } else if (!ListUtils.equalsWithoutOrder(routines, other.routines)) {
                        return false;
                }
                if (tables == null) {
                        if (other.tables != null) {
                                return false;
                        }
                } else if (!ListUtils.equalsWithoutOrder(tables, other.tables)) {
                        return false;
                }
                if (views == null) {
                        if (other.views != null) {
                                return false;
                        }
                } else if (!ListUtils.equalsWithoutOrder(views, other.views)) {
                        return false;
                }
                return true;
        }

        public int getIndex() {
                return index;
        }

        public void setIndex(int index) {
                this.index = index;
        }
}
