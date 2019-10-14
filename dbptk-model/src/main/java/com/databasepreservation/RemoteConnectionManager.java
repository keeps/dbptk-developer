package com.databasepreservation;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import java.io.InputStream;
import java.nio.file.Path;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class RemoteConnectionManager {

  private String host, username, password, portNumber;
  private static boolean configured;
  private static RemoteConnectionManager instance = null;
  private ChannelSftp channelSftp = null;

  private RemoteConnectionManager(){}

  public static RemoteConnectionManager getInstance(){
    if(instance == null){
      instance = new RemoteConnectionManager();
    }
    return instance;
  }

  public void setup(String host, String username, String password, String portNumber) {
    if (!configured) {
      this.host = host;
      this.username = username;
      this.password = password;
      this.portNumber = portNumber;
      configured = true;
    }
  }

  public boolean isConfigured() {
    return configured;
  }

  public InputStream getInputStream(Path blobPath) throws JSchException, SftpException {
    if (channelSftp == null) {
      JSch jsch = new JSch();
      Session session = jsch.getSession(username, host, Integer.parseInt(portNumber));

      session.setPassword(password);
      session.setConfig("StrictHostKeyChecking", "no");
      session.connect();

      Channel channel = session.openChannel("sftp");
      channel.connect();
      channelSftp = (ChannelSftp) channel;
    }

    return channelSftp.get(blobPath.normalize().toString());
  }
}
