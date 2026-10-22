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
import java.nio.charset.Charset;

import org.apache.commons.lang3.StringUtils;

import io.github.spannm.jackcess.Database;
import io.github.spannm.jackcess.DatabaseBuilder;
import io.github.spannm.jackcess.DateTimeType;
import io.github.spannm.jackcess.encrypt.CryptCodecProvider;
import net.ucanaccess.jdbc.IJackcessOpenerInterface;

/**
 * {@link IJackcessOpenerInterface} implementation that registers a
 * {@link CryptCodecProvider} so that password-protected Microsoft Access
 * databases (including the AES-encrypted format used by Access 2007+
 * {@code .accdb} files) can actually be decoded. UCanAccess itself never wires
 * up {@code jackcess-encrypt}, so without a custom opener like this one, any
 * password passed to UCanAccess is silently ignored.
 */
public class MsAccessCryptCodecJackcessOpener implements IJackcessOpenerInterface {

  @Override
  public Database open(File fl, String pwd) throws IOException {
    return open(fl, pwd, null);
  }

  @Override
  public Database open(File fl, String pwd, Charset charset) throws IOException {
    DatabaseBuilder dbd = new DatabaseBuilder()
      .withFile(fl)
      .withAutoSync(false)
      .withCharset(charset);

    if (StringUtils.isNotEmpty(pwd)) {
      dbd.withCodecProvider(new CryptCodecProvider(pwd));
    }

    Database db;
    try {
      db = dbd.withReadOnly(false).open();
    } catch (Exception ex) {
      db = dbd.withReadOnly(true).open();
    }
    db.setDateTimeType(DateTimeType.LOCAL_DATE_TIME);
    return db;
  }

}
