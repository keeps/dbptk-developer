/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.msAccess.in;

import java.io.File;
import java.io.IOException;

import com.healthmarketscience.jackcess.crypt.CryptCodecProvider;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;

import net.ucanaccess.jdbc.JackcessOpenerInterface;

/**
 * Used to open MsAccess databases that are protected by a password.
 *
 * @see <a href=
 *      "http://stackoverflow.com/questions/31429939/how-to-connect-ucanaccess-to-an-access-database-encrypted-with-a-database-passwo">source1</a>
 * @see <a href=
 *      "https://www.developpez.net/forums/d1546199/java/general-java/jdbc/ucanaccess-utilisation-base-access-protegee/">source2</a>
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class CryptCodecOpener implements JackcessOpenerInterface {
  public Database open(File fl, String pwd) throws IOException {
    DatabaseBuilder dbd = new DatabaseBuilder(fl);
    dbd.setAutoSync(false);
    dbd.setCodecProvider(new CryptCodecProvider(pwd));
    dbd.setReadOnly(false);
    return dbd.open();
  }
}
