package dk.magenta.common;

import javax.xml.bind.Marshaller;

import com.databasepreservation.model.exception.ModuleException;

public interface MarshallerGenerator {
	
	/**
	 * Generate JAXB Marshaller for writing XML object to the archive.  
	 * @param context Parameter to give to JAXBContext.newInstance()
	 * @param localeSchemaLocation The locale location of the XML schema for the metadata file.
	 * @param JAXBSchemaLocation The Marshaller.JAXB_SCHEMA_LOCATION.
	 */
	public Marshaller generateMarshaller(String context, String localeSchemaLocation, String JAXBSchemaLocation) throws ModuleException;
}
