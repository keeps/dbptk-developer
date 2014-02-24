package pt.gov.dgarq.roda.common.convert.db.model.structure;

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDDatabaseStructure {

	private static Logger logger = Logger.
			getLogger(SIARDDatabaseStructure.class);
	
	private String version; 
	
	private String dbname;
	
	private String description;
	
	private String archiver;
	
	private String archiverContact;
	
	private String dataOwner;
	
	private String dataOriginTimespan;
	
	private String producerApplication;
	
	private Date archivalDate;
	
	private String messageDigest;
	
	private String clientMachine;
	
	private String databaseProduct;
	
	private String connection;
	
	private String databaseUser;
	
	private List<SIARDSchemaStructure> schemas;
	
	private List<SIARDUserStructure> users;
	
	private List<SIARDRoleStructure> roles;
	
	private List<SIARDPrivilegeStructure> privileges;

	/**
	 * 
	 */
	public SIARDDatabaseStructure() {
		//TODO Complete
	}

	/**
	 * @param version
	 * @param dbname
	 * @param dataOwner
	 * @param dataOriginTimespan
	 * @param archivalDate
	 * @param messageDigest
	 * @param schemas
	 * @param users
	 */
	public SIARDDatabaseStructure(String version, String dbname,
			String dataOwner, String dataOriginTimespan, Date archivalDate,
			String messageDigest, List<SIARDSchemaStructure> schemas, 
			List<SIARDUserStructure> users) {
		super();
		this.version = version;
		this.dbname = dbname;
		this.dataOwner = dataOwner;
		this.dataOriginTimespan = dataOriginTimespan;
		this.archivalDate = archivalDate;
		this.messageDigest = messageDigest;
		this.schemas = schemas;
		this.users = users;
	}


	/**
	 * @return the version
	 */
	public String getVersion() {
		return version;
	}


	/**
	 * @param version 
	 * 			the version to set
	 */
	public void setVersion(String version) {
		this.version = version;
	}


	/**
	 * @return the dbname
	 */
	public String getDbname() {
		return dbname;
	}


	/**
	 * @param dbname 
	 * 			the dbname to set
	 */
	public void setDbname(String dbname) {
		this.dbname = dbname;
	}


	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}


	/**
	 * @param description 
	 * 			the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}


	/**
	 * @return the archiver
	 */
	public String getArchiver() {
		return archiver;
	}


	/**
	 * @param archiver 
	 * 			the archiver to set
	 */
	public void setArchiver(String archiver) {
		this.archiver = archiver;
	}


	/**
	 * @return the archiverContact
	 */
	public String getArchiverContact() {
		return archiverContact;
	}


	/**
	 * @param archiverContact 
	 * 			the archiverContact to set
	 */
	public void setArchiverContact(String archiverContact) {
		this.archiverContact = archiverContact;
	}


	/**
	 * @return the dataOwner
	 */
	public String getDataOwner() {
		return dataOwner;
	}


	/**
	 * @param dataOwner 
	 * 			the dataOwner to set
	 */
	public void setDataOwner(String dataOwner) {
		this.dataOwner = dataOwner;
	}


	/**
	 * @return the dataOriginTimespan
	 */
	public String getDataOriginTimespan() {
		return dataOriginTimespan;
	}


	/**
	 * @param dataOriginTimespan 
	 * 			the dataOriginTimespan to set
	 */
	public void setDataOriginTimespan(String dataOriginTimespan) {
		this.dataOriginTimespan = dataOriginTimespan;
	}


	/**
	 * @return the producerApplication
	 */
	public String getProducerApplication() {
		return producerApplication;
	}


	/**
	 * @param producerApplication 
	 * 			the producerApplication to set
	 */
	public void setProducerApplication(String producerApplication) {
		this.producerApplication = producerApplication;
	}


	/**
	 * @return the archivalDate
	 */
	public Date getArchivalDate() {
		return archivalDate;
	}


	/**
	 * @param archivalDate 
	 * 			the archivalDate to set
	 */
	public void setArchivalDate(Date archivalDate) {
		this.archivalDate = archivalDate;
	}


	/**
	 * @return the messageDigest
	 */
	public String getMessageDigest() {
		return messageDigest;
	}


	/**
	 * @param messageDigest 
	 * 			the messageDigest to set
	 */
	public void setMessageDigest(String messageDigest) {
		this.messageDigest = messageDigest;
	}


	/**
	 * @return the clienteMachine
	 */
	public String getClientMachine() {
		return clientMachine;
	}


	/**
	 * @param clienteMachine 
	 * 			the clienteMachine to set
	 */
	public void setClientMachine(String clientMachine) {
		this.clientMachine = clientMachine;
	}


	/**
	 * @return the databaseProduct
	 */
	public String getDatabaseProduct() {
		return databaseProduct;
	}


	/**
	 * @param databaseProduct 
	 * 			the databaseProduct to set
	 */
	public void setDatabaseProduct(String databaseProduct) {
		this.databaseProduct = databaseProduct;
	}


	/**
	 * @return the connection
	 */
	public String getConnection() {
		return connection;
	}


	/**
	 * @param connection 
	 * 			the connection to set
	 */
	public void setConnection(String connection) {
		this.connection = connection;
	}


	/**
	 * @return the databaseUser
	 */
	public String getDatabaseUser() {
		return databaseUser;
	}


	/**
	 * @param databaseUser 
	 * 			the databaseUser to set
	 */
	public void setDatabaseUser(String databaseUser) {
		this.databaseUser = databaseUser;
	}


	/**
	 * @return the schemas
	 */
	public List<SIARDSchemaStructure> getSchemas() {
		return schemas;
	}


	/**
	 * @param schemas 
	 * 			the schemas to set
	 */
	public void setSchemas(List<SIARDSchemaStructure> schemas) {
		this.schemas = schemas;
	}


	/**
	 * @return the users
	 */
	public List<SIARDUserStructure> getUsers() {
		return users;
	}


	/**
	 * @param users 
	 * 			the users to set
	 */
	public void setUsers(List<SIARDUserStructure> users) {
		this.users = users;
	}
	

	/**
	 * @return the roles
	 */
	public List<SIARDRoleStructure> getRoles() {
		return roles;
	}


	/**
	 * @param roles the roles to set
	 */
	public void setRoles(List<SIARDRoleStructure> roles) {
		this.roles = roles;
	}


	/**
	 * @return the privileges
	 */
	public List<SIARDPrivilegeStructure> getPrivileges() {
		return privileges;
	}


	/**
	 * @param privileges the privileges to set
	 */
	public void setPrivileges(List<SIARDPrivilegeStructure> privileges) {
		this.privileges = privileges;
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{ version: ");
		builder.append(version);
		builder.append(", dbname: ");
		builder.append(dbname);
		builder.append(", description:");
		builder.append(description);
		builder.append(", archiver:");
		builder.append(archiver);
		builder.append(", archiverContact:");
		builder.append(archiverContact);
		builder.append(", dataOwner:");
		builder.append("\"");
		builder.append(dataOwner);
		builder.append("\"");
		builder.append(", dataOriginTimespan:");
		builder.append("\"");
		builder.append(dataOriginTimespan);
		builder.append("\"");
		builder.append(", producerApplication:");
		builder.append("\"");
		builder.append(producerApplication);
		builder.append("\"");
		builder.append(", archivalDate:");
		builder.append("\"");
		builder.append(archivalDate);
		builder.append("\"");
		builder.append(", messageDigest:");
		builder.append("\"");
		builder.append(messageDigest);
		builder.append("\"");
		builder.append(", clientMachine:");
		builder.append("\"");
		builder.append(clientMachine);
		builder.append("\"");
		builder.append(", databaseProduct:");
		builder.append("\"");
		builder.append(databaseProduct);
		builder.append("\"");
		builder.append(", connection:");
		builder.append("\"");
		builder.append(connection);
		builder.append("\"");
		builder.append(", databaseUser:");
		builder.append(databaseUser);
		builder.append(", schemas:");
		builder.append("\"");
		builder.append(schemas);
		builder.append("\"");
//		builder.append(", users:");
//		builder.append("\"");
//		builder.append(users);
//		builder.append("\"");
		builder.append("}");
		return  builder.toString();
	}
	
}
