/**
 *
 */
package com.databasepreservation.model.structure.type;

/**
 * A sequence of values of the same type.
 *
 * @author Luis Faria
 */
public class ComposedTypeArray extends Type {
        private Type elementType;

        /**
         * Empty Array type constructor.
         */
        public ComposedTypeArray() {
        }

        /**
         * Array type constructor.
         *
         * @param elementType The type of the elements within this array (required).
         */
        public ComposedTypeArray(Type elementType) {
                this.elementType = elementType;
        }

        /**
         * @return The type of the elements within this array
         */
        public Type getElementType() {
                return elementType;
        }

        /**
         * @param elementType The type of the elements within this array
         */
        public void setElementType(Type elementType) {
                this.elementType = elementType;
        }

        @Override public int hashCode() {
                final int prime = 31;
                int result = super.hashCode();
                result = prime * result + ((elementType == null) ? 0 : elementType.hashCode());
                return result;
        }

        @Override public boolean equals(Object obj) {
                if (this == obj) {
                        return true;
                }
                if (!super.equals(obj)) {
                        return false;
                }
                if (getClass() != obj.getClass()) {
                        return false;
                }
                ComposedTypeArray other = (ComposedTypeArray) obj;
                if (elementType == null) {
                        if (other.elementType != null) {
                                return false;
                        }
                } else if (!elementType.equals(other.elementType)) {
                        return false;
                }
                return true;
        }

}
