package dk.magenta.common;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.xml.sax.SAXException;

import com.databasepreservation.model.exception.ModuleException;

public class StandardMarshallerGenerator implements MarshallerGenerator {

	private static final String ENCODING = "UTF-8";
	
	@Override
	public Marshaller generateMarshaller(String contextStr, String localeSchemaLocation,	String JAXBSchemaLocation) throws ModuleException {
		
		// Set up JAXB marshaller 
		
		JAXBContext context;
		try {
			context = JAXBContext.newInstance(contextStr);
		} catch (JAXBException e) {
			throw new ModuleException("Error loading JAXBContent", e);
		}
		
		SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
		Schema xsdSchema = null;
		try {
			InputStream in = this.getClass().getResourceAsStream(localeSchemaLocation);
			xsdSchema = schemaFactory.newSchema(new StreamSource(in));
			in.close();
		} catch (SAXException e) {
			throw new ModuleException("XSD file has errors: " + getClass().getResource(localeSchemaLocation).getPath(), e);
		} catch (IOException e) {
			throw new ModuleException("Could not close InputStream", e);
		}

		Marshaller m = null;
		
		try {
			
			m = context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
		    m.setProperty(Marshaller.JAXB_ENCODING, ENCODING);
			m.setProperty(Marshaller.JAXB_SCHEMA_LOCATION, JAXBSchemaLocation);

			m.setSchema(xsdSchema);
			
		} catch (JAXBException e) {
			throw new ModuleException("Error while Marshalling JAXB", e);
		}
		
		return m;
	}
}
