/**
 *
 */
package com.databasepreservation.model.structure.type;

/**
 * An exact numeric includes integer types and types with specified precision
 * (number of digits) and scale (digits after the radix point)
 *
 * @author Luis Faria
 */
public class SimpleTypeNumericExact extends Type {
        private Integer precision;

        private Integer scale;

        /**
         * Exact numeric, like int or integer
         */
        public SimpleTypeNumericExact() {

        }

        /**
         * Exact numeric, like int or integer, with optional fields.
         *
         * @param precision the number of digits (optional)
         * @param scale     the number of digits after the radix point (optional)
         */
        public SimpleTypeNumericExact(Integer precision, Integer scale) {
                this.precision = precision;
                this.scale = scale;
        }

        /**
         * @return the number of digits, or null if undefined
         */
        public Integer getPrecision() {
                return precision;
        }

        /**
         * @param precision the number of digits, or null if undefined
         */
        public void setPrecision(Integer precision) {
                this.precision = precision;
        }

        /**
         * @return the number of digits after the radix point, or null if undefined
         */
        public Integer getScale() {
                return scale;
        }

        /**
         * @param scale the number of digits after the radix point, or null if
         *              undefined
         */
        public void setScale(Integer scale) {
                this.scale = scale;
        }

        @Override public int hashCode() {
                final int prime = 31;
                int result = super.hashCode();
                result = prime * result + ((precision == null) ? 0 : precision.hashCode());
                result = prime * result + ((scale == null) ? 0 : scale.hashCode());
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
                SimpleTypeNumericExact other = (SimpleTypeNumericExact) obj;
                if (precision == null) {
                        if (other.precision != null) {
                                return false;
                        }
                } else if (!precision.equals(other.precision)) {
                        return false;
                }
                if (scale == null) {
                        if (other.scale != null) {
                                return false;
                        }
                } else if (!scale.equals(other.scale)) {
                        return false;
                }
                return true;
        }
}
