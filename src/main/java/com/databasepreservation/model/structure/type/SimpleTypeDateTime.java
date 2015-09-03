/**
 *
 */
package com.databasepreservation.model.structure.type;

/**
 * Date and time according to ISO 8601.
 *
 * @author Luis Faria
 */
public class SimpleTypeDateTime extends Type {

        private Boolean timeDefined;

        private Boolean timeZoneDefined;

        /**
         * DateTime type constructor. All fields are required.
         *
         * @param timeDefined     If time is defined in date time declaration, i.e. hour,
         *                        minutes, seconds or milliseconds.
         * @param timeZoneDefined If time zone is defined in date time declaration.
         */
        public SimpleTypeDateTime(Boolean timeDefined, Boolean timeZoneDefined) {
                this.timeDefined = timeDefined;
                this.timeZoneDefined = timeZoneDefined;
        }

        /**
         * @return If time is defined in date time declaration, i.e. hour, minutes,
         * seconds or milliseconds.
         */
        public Boolean getTimeDefined() {
                return timeDefined;
        }

        /**
         * @param timeDefined If time is defined in date time declaration, i.e. hour,
         *                    minutes, seconds or milliseconds.
         */
        public void setTimeDefined(Boolean timeDefined) {
                this.timeDefined = timeDefined;
        }

        /**
         * @return If time zone is defined in date time declaration.
         */
        public Boolean getTimeZoneDefined() {
                return timeZoneDefined;
        }

        /**
         * @param timeZoneDefined If time zone is defined in date time declaration.
         */
        public void setTimeZoneDefined(Boolean timeZoneDefined) {
                this.timeZoneDefined = timeZoneDefined;
        }

        @Override public int hashCode() {
                final int prime = 31;
                int result = super.hashCode();
                result = prime * result + ((timeDefined == null) ? 0 : timeDefined.hashCode());
                result = prime * result + ((timeZoneDefined == null) ? 0 : timeZoneDefined.hashCode());
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
                SimpleTypeDateTime other = (SimpleTypeDateTime) obj;
                if (timeDefined == null) {
                        if (other.timeDefined != null) {
                                return false;
                        }
                } else if (!timeDefined.equals(other.timeDefined)) {
                        return false;
                }
                if (timeZoneDefined == null) {
                        if (other.timeZoneDefined != null) {
                                return false;
                        }
                } else if (!timeZoneDefined.equals(other.timeZoneDefined)) {
                        return false;
                }
                return true;
        }

}
