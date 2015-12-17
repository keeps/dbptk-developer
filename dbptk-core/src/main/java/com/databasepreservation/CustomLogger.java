package com.databasepreservation;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.ResourceBundle;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.Category;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.log4j.net.SocketAppender;
import org.apache.log4j.or.ObjectRenderer;
import org.apache.log4j.spi.LoggerFactory;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Defines a different log behaviour based on log4j and specific to this
 * project. This assumes that the STDOUT has a log threshold of INFO and the log
 * file has a log threshold of DEBUG. This new behaviour is only defined when a
 * Throwable is given.
 *
 * TRACE - delegated to log4j
 * 
 * DEBUG - delegated to log4j
 * 
 * INFO - delegated to log4j
 *
 * WARN (non-throwable) - delegated to log4j
 *
 * WARN (message and throwable) - logs twice: first an user-friendly WARN
 * message and then logs stack traces and other info as DEBUG
 *
 * ERROR (non-throwable) - delegated to log4j
 *
 * ERROR (message and throwable) - logs twice: first an user-friendly ERROR
 * message and then logs stack traces and other info as DEBUG
 *
 * FATAL (non-throwable) - delegated to log4j
 *
 * FATAL (message and throwable) - logs twice: first an user-friendly FATAL
 * message and then logs stack traces and other info as DEBUG
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class CustomLogger {
  // Delegate logger
  private final Logger logger;

  private static final String NEWLINE = System.getProperty("line.separator");
  private static final String INDENT = StringUtils.repeat(' ', 17);

  // not used
  private CustomLogger() {
    logger = null;
  }

  /**
   * Create a custom logger that uses a log4j logger
   * 
   * @param logger
   *          the base logger
   */
  private CustomLogger(Logger logger) {
    this.logger = logger;
  }

  /**
   * If message is a throwable object: Logs WARN with throwable message and then
   * logs DEBUG with throwable
   *
   * If message is not a throwable object: delegates method call to log4j
   *
   * @param message
   *          the message object to log.
   */
  public void warn(Object message) {
    if (message instanceof Throwable) {
      Throwable throwable = (Throwable) message;
      this.warn(throwable, throwable);
    } else {
      logger.warn(message);
    }
  }

  /**
   * Logs WARN with message and then logs DEBUG with message and throwable
   *
   * @param message
   *          the message object to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void warn(Object message, Throwable t) {
    if (message instanceof Throwable) {
      Throwable badMessage = (Throwable) message;
      message = badMessage.getMessage();
    } else {
      message = message.toString();
    }
    logger.warn(message);
    logger.debug(message, t);
  }

  /**
   * If message is a throwable object: Logs ERROR with throwable message and
   * then logs DEBUG with throwable
   *
   * If message is not a throwable object: delegates method call to log4j
   *
   * @param message
   *          the message object to log.
   */
  public void error(Object message) {
    if (message instanceof Throwable) {
      Throwable throwable = (Throwable) message;
      this.error(throwable, throwable);
    } else {
      logger.error(message);
    }
  }

  /**
   * Logs ERROR with message and then logs DEBUG with message and throwable
   *
   * @param message
   *          the message object to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void error(Object message, Throwable t) {
    if (message instanceof Throwable) {
      Throwable badMessage = (Throwable) message;
      message = getUserFriendlyMessage(badMessage);
    } else {
      message = message.toString() + getUserFriendlyMessage(t);
    }
    logger.error(message);
    logger.debug(message, t);
  }

  /**
   * If message is a throwable object: Logs FATAL with throwable message and
   * then logs DEBUG with throwable
   *
   * If message is not a throwable object: delegates method call to log4j
   *
   * @param message
   *          the message object to log.
   */
  public void fatal(Object message) {
    if (message instanceof Throwable) {
      Throwable throwable = (Throwable) message;
      this.fatal(throwable, throwable);
    } else {
      logger.fatal(message);
    }
  }

  /**
   * Logs FATAL with message and then logs DEBUG with message and throwable
   *
   * @param message
   *          the message object to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void fatal(Object message, Throwable t) {
    if (message instanceof Throwable) {
      Throwable badMessage = (Throwable) message;
      message = getUserFriendlyMessage(badMessage);
    } else {
      message = message.toString() + getUserFriendlyMessage(t);
    }
    logger.fatal(message);
    logger.debug(message, t);
  }

  /**
   * Given a message and a Throwable, produces an user-friendly (yet detailed)
   * message describing the problem
   * 
   * @param message
   *          a short description of the problem
   * @param throwable
   *          the throwable to extract the message from
   * @return the user-friendly message
   */
  private String getUserFriendlyMessage(Object message, Throwable throwable) {
    return message.toString() + getUserFriendlyMessage(throwable);
  }

  /**
   * Given some Throwable, produces an user-friendly (yet detailed) message
   * describing the problem
   * 
   * @param throwable
   *          the throwable to extract the message from
   * @return the user-friendly message
   */
  private String getUserFriendlyMessage(Throwable throwable) {
    StringBuilder message = new StringBuilder();

    // get all throwables inside this throwable (through "getCause()")
    Throwable actual = throwable;
    ArrayList<Throwable> throwables = new ArrayList<>();
    while (actual != null) {
      if (throwables.contains(actual)) {
        actual = null;
      } else {
        throwables.add(actual);
        actual = actual.getCause();
      }
    }

    for (Throwable t : throwables) {
      String msg = t.getMessage();
      if (StringUtils.isNotBlank(msg)) {
        String formattedMessage = msg.replaceAll("[ ]*[\\r\\n]+", NEWLINE + INDENT);
        message.append(NEWLINE).append(INDENT).append(formattedMessage);
      }
    }

    return message.toString();
  }

  /**
   * Shorthand for <code>getLogger(clazz.getName())</code>.
   *
   * @param clazz
   *          The name of <code>clazz</code> will be used as the name of the
   *          logger to retrieve. See {@link #getLogger(String)} for more
   *          detailed information.
   */
  public static CustomLogger getLogger(Class clazz) {
    return new CustomLogger(Logger.getLogger(clazz));
  }

  /**
   * Retrieve a logger named according to the value of the <code>name</code>
   * parameter. If the named logger already exists, then the existing instance
   * will be returned. Otherwise, a new instance is created.
   *
   * <p>
   * By default, loggers do not have a set level but inherit it from their
   * neareast ancestor with a set level. This is one of the central features of
   * log4j.
   *
   * @param name
   *          The name of the logger to retrieve.
   */
  public static CustomLogger getLogger(String name) {
    return new CustomLogger(Logger.getLogger(name));
  }

  /**
   * Like {@link #getLogger(String)} except that the type of logger instantiated
   * depends on the type returned by the
   * {@link LoggerFactory#makeNewLoggerInstance} method of the
   * <code>factory</code> parameter.
   *
   * <p>
   * This method is intended to be used by sub-classes.
   *
   * @since 0.8.5
   * @param name
   *          The name of the logger to retrieve.
   * @param factory
   *          A {@link LoggerFactory} implementation that will actually create a
   *          new Instance.
   */
  public static CustomLogger getLogger(String name, LoggerFactory factory) {
    return new CustomLogger(Logger.getLogger(name, factory));
  }

  /**
   * Return the root logger for the current logger repository.
   * <p>
   * The {@link #getName Logger.getName()} method for the root logger always
   * returns stirng value: "root". However, calling
   * <code>Logger.getLogger("root")</code> does not retrieve the root logger but
   * a logger just under root named "root".
   * <p>
   * In other words, calling this method is the only way to retrieve the root
   * logger.
   */
  public static CustomLogger getRootLogger() {
    return new CustomLogger(Logger.getRootLogger());
  }

  /*-------------------------------------------------
              DELEGATED METHODS
   ------------------------------------------------*/

  /**
   * Set the additivity flag for this Category instance.
   * 
   * @since 0.8.1
   * @param additive
   */
  public void setAdditivity(boolean additive) {
    logger.setAdditivity(additive);
  }

  /**
   * Returns the parent of this category. Note that the parent of a given
   * category may change during the lifetime of the category.
   * 
   * <p>
   * The root category will return <code>null</code>.
   * 
   * @since 1.2
   */
  public Category getParent() {
    return logger.getParent();
  }

  /**
   * Add <code>newAppender</code> to the list of appenders of this Category
   * instance.
   * 
   * <p>
   * If <code>newAppender</code> is already in the list of appenders, then it
   * won't be added again.
   * 
   * @param newAppender
   */
  public void addAppender(Appender newAppender) {
    logger.addAppender(newAppender);
  }

  /**
   * Log a message object with the {@link Level#TRACE TRACE} level.
   *
   * @param message
   *          the message object to log.
   * @see #debug(Object) for an explanation of the logic applied.
   * @since 1.2.12
   */
  public void trace(Object message) {
    logger.trace(message);
  }

  /**
   * @deprecated Make sure to use {@link Logger#getLogger(String)} instead.
   * @param name
   */
  @Deprecated
  public static Category getInstance(String name) {
    return Category.getInstance(name);
  }

  /**
   * Remove the appender with the name passed as parameter form the list of
   * appenders.
   * 
   * @since 0.8.2
   * @param name
   */
  public void removeAppender(String name) {
    logger.removeAppender(name);
  }

  /**
   * @deprecated Please use {@link Logger#getRootLogger()} instead.
   */
  @Deprecated
  public static Category getRoot() {
    return Category.getRoot();
  }

  /**
   * Remove the appender passed as parameter form the list of appenders.
   * 
   * @since 0.8.2
   * @param appender
   */
  public void removeAppender(Appender appender) {
    logger.removeAppender(appender);
  }

  /**
   * If <code>assertion</code> parameter is <code>false</code>, then logs
   * <code>msg</code> as an {@link #error(Object) error} statement.
   * 
   * <p>
   * The <code>assert</code> method has been renamed to <code>assertLog</code>
   * because <code>assert</code> is a language reserved word in JDK 1.4.
   * 
   * @since 1.2
   * @param assertion
   * @param msg
   *          The message to print if <code>assertion</code> is false.
   */
  public void assertLog(boolean assertion, String msg) {
    logger.assertLog(assertion, msg);
  }

  /**
   * Starting from this category, search the category hierarchy for a non-null
   * level and return it. Otherwise, return the level of the root category.
   * 
   * <p>
   * The Category class is designed so that this method executes as quickly as
   * possible.
   */
  public Level getEffectiveLevel() {
    return logger.getEffectiveLevel();
  }

  /**
   * Log a localized and parameterized message. First, the user supplied
   * <code>key</code> is searched in the resource bundle. Next, the resulting
   * pattern is formatted using {@link MessageFormat#format(String, Object[])}
   * method with the user supplied object array <code>params</code>.
   * 
   * @since 0.8.4
   * @param priority
   * @param key
   * @param params
   * @param t
   */
  public void l7dlog(Priority priority, String key, Object[] params, Throwable t) {
    logger.l7dlog(priority, key, params, t);
  }

  /**
   * Look for the appender named as <code>name</code>.
   * 
   * <p>
   * Return the appender with that name if in the list. Return <code>null</code>
   * otherwise.
   * 
   * @param name
   */
  public Appender getAppender(String name) {
    return logger.getAppender(name);
  }

  /**
   * Set the level of this Category. If you are passing any of
   * <code>Level.DEBUG</code>, <code>Level.INFO</code>, <code>Level.WARN</code>,
   * <code>Level.ERROR</code>, <code>Level.FATAL</code> as a parameter, you need
   * to case them as Level.
   * 
   * <p>
   * As in
   * 
   * <pre>
   * logger.setLevel((Level) Level.DEBUG);
   * </pre>
   * 
   * 
   * <p>
   * Null values are admitted.
   * 
   * @param level
   */
  public void setLevel(Level level) {
    logger.setLevel(level);
  }

  /**
   * Log a message object with the <code>TRACE</code> level including the stack
   * trace of the {@link Throwable}<code>t</code> passed as parameter.
   *
   * <p>
   * See {@link #debug(Object)} form for more detailed information.
   * </p>
   *
   * @param message
   *          the message object to log.
   * @param t
   *          the exception to log, including its stack trace.
   * @since 1.2.12
   */
  public void trace(Object message, Throwable t) {
    logger.trace(message, t);
  }

  /**
   * Check whether this category is enabled for the <code>DEBUG</code> Level.
   *
   * <p>
   * This function is intended to lessen the computational cost of disabled log
   * debug statements.
   *
   * <p>
   * For some <code>cat</code> Category object, when you write,
   * 
   * <pre>
   * cat.debug(&quot;This is entry number: &quot; + i);
   * </pre>
   *
   * <p>
   * You incur the cost constructing the message, concatenatiion in this case,
   * regardless of whether the message is logged or not.
   *
   * <p>
   * If you are worried about speed, then you should write
   * 
   * <pre>
   * if (cat.isDebugEnabled()) {
   *   cat.debug(&quot;This is entry number: &quot; + i);
   * }
   * </pre>
   *
   * <p>
   * This way you will not incur the cost of parameter construction if debugging
   * is disabled for <code>cat</code>. On the other hand, if the
   * <code>cat</code> is debug enabled, you will incur the cost of evaluating
   * whether the category is debug enabled twice. Once in
   * <code>isDebugEnabled</code> and once in the <code>debug</code>. This is an
   * insignificant overhead since evaluating a category takes about 1%% of the
   * time it takes to actually log.
   *
   * @return boolean - <code>true</code> if this category is debug enabled,
   *         <code>false</code> otherwise.
   * */
  public boolean isDebugEnabled() {
    return logger.isDebugEnabled();
  }

  /**
   * Log a message object with the <code>DEBUG</code> level including the stack
   * trace of the {@link Throwable} <code>t</code> passed as parameter.
   * 
   * <p>
   * See {@link #debug(Object)} form for more detailed information.
   * 
   * @param message
   *          the message object to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void debug(Object message, Throwable t) {
    logger.debug(message, t);
  }

  /**
   *
   * @deprecated Please use the the {@link #getEffectiveLevel} method instead.
   * */
  @Deprecated
  public Priority getChainedPriority() {
    return logger.getChainedPriority();
  }

  /**
   * This generic form is intended to be used by wrappers.
   * 
   * @param priority
   * @param message
   * @param t
   */
  public void log(Priority priority, Object message, Throwable t) {
    logger.log(priority, message, t);
  }

  /**
   * Set the level of this Category.
   * 
   * <p>
   * Null values are admitted.
   * 
   * @deprecated Please use {@link #setLevel} instead.
   * @param priority
   */
  @Deprecated
  public void setPriority(Priority priority) {
    logger.setPriority(priority);
  }

  /**
   * Return the default Hierarchy instance.
   * 
   * @deprecated Please use {@link LogManager#getLoggerRepository()} instead.
   * @since 1.0
   */
  @Deprecated
  public static LoggerRepository getDefaultHierarchy() {
    return Category.getDefaultHierarchy();
  }

  /**
   * If the named category exists (in the default hierarchy) then it returns a
   * reference to the category, otherwise it returns <code>null</code>.
   * 
   * @deprecated Please use {@link LogManager#exists} instead.
   * @since 0.8.5
   * @param name
   */
  @Deprecated
  public static Logger exists(String name) {
    return Category.exists(name);
  }

  /**
   * Calling this method will <em>safely</em> close and remove all appenders in
   * all the categories including root contained in the default hierachy.
   * 
   * <p>
   * Some appenders such as {@link SocketAppender} and {@link AsyncAppender}
   * need to be closed before the application exists. Otherwise, pending logging
   * events might be lost.
   * 
   * <p>
   * The <code>shutdown</code> method is careful to close nested appenders
   * before closing regular appenders. This is allows configurations where a
   * regular appender is attached to a category and again to a nested appender.
   * 
   * @deprecated Please use {@link LogManager#shutdown()} instead.
   * @since 1.0
   */
  @Deprecated
  public static void shutdown() {
    Category.shutdown();
  }

  /**
   * Get the appenders contained in this category as an {@link Enumeration}. If
   * no appenders can be found, then a {@link NullEnumeration} is returned.
   * 
   * @return Enumeration An enumeration of the appenders in this category.
   */
  public Enumeration getAllAppenders() {
    return logger.getAllAppenders();
  }

  /**
   * Check whether this category is enabled for the info Level. See also
   * {@link #isDebugEnabled}.
   * 
   * @return boolean - <code>true</code> if this category is enabled for level
   *         info, <code>false</code> otherwise.
   */
  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  /**
   * Log a message object with the {@link Level#DEBUG DEBUG} level.
   * 
   * <p>
   * This method first checks if this category is <code>DEBUG</code> enabled by
   * comparing the level of this category with the {@link Level#DEBUG DEBUG}
   * level. If this category is <code>DEBUG</code> enabled, then it converts the
   * message object (passed as parameter) to a string by invoking the
   * appropriate {@link ObjectRenderer}. It then proceeds to call all the
   * registered appenders in this category and also higher in the hierarchy
   * depending on the value of the additivity flag.
   * 
   * <p>
   * <b>WARNING</b> Note that passing a {@link Throwable} to this method will
   * print the name of the <code>Throwable</code> but no stack trace. To print a
   * stack trace use the {@link #debug(Object, Throwable)} form instead.
   * 
   * @param message
   *          the message object to log.
   */
  public void debug(Object message) {
    logger.debug(message);
  }

  /**
   * @deprecated Please make sure to use {@link Logger#getLogger(Class)}
   *             instead.
   * @param clazz
   */
  @Deprecated
  public static Category getInstance(Class clazz) {
    return Category.getInstance(clazz);
  }

  /**
   * This generic form is intended to be used by wrappers.
   * 
   * @param priority
   * @param message
   */
  public void log(Priority priority, Object message) {
    logger.log(priority, message);
  }

  /**
   * Return the category name.
   */
  public String getName() {
    return logger.getName();
  }

  /**
   * Log a message object with the {@link Level#INFO INFO} Level.
   * 
   * <p>
   * This method first checks if this category is <code>INFO</code> enabled by
   * comparing the level of this category with {@link Level#INFO INFO} Level. If
   * the category is <code>INFO</code> enabled, then it converts the message
   * object passed as parameter to a string by invoking the appropriate
   * {@link ObjectRenderer}. It proceeds to call all the registered appenders in
   * this category and also higher in the hierarchy depending on the value of
   * the additivity flag.
   * 
   * <p>
   * <b>WARNING</b> Note that passing a {@link Throwable} to this method will
   * print the name of the Throwable but no stack trace. To print a stack trace
   * use the {@link #info(Object, Throwable)} form instead.
   * 
   * @param message
   *          the message object to log
   */
  public void info(Object message) {
    logger.info(message);
  }

  /**
   * Get the additivity flag for this Category instance.
   */
  public boolean getAdditivity() {
    return logger.getAdditivity();
  }

  /**
   * Returns the assigned {@link Level}, if any, for this Category.
   * 
   * @return Level - the assigned Level, can be <code>null</code>.
   */
  public Level getLevel() {
    return logger.getLevel();
  }

  /**
   * Return the <em>inherited</em> {@link ResourceBundle} for this category.
   * 
   * <p>
   * This method walks the hierarchy to find the appropriate resource bundle. It
   * will return the resource bundle attached to the closest ancestor of this
   * category, much like the way priorities are searched. In case there is no
   * bundle in the hierarchy then <code>null</code> is returned.
   * 
   * @since 0.9.0
   */
  public ResourceBundle getResourceBundle() {
    return logger.getResourceBundle();
  }

  /**
   * Log a message object with the <code>INFO</code> level including the stack
   * trace of the {@link Throwable} <code>t</code> passed as parameter.
   * 
   * <p>
   * See {@link #info(Object)} for more detailed information.
   * 
   * @param message
   *          the message object to log.
   * @param t
   *          the exception to log, including its stack trace.
   */
  public void info(Object message, Throwable t) {
    logger.info(message, t);
  }

  /**
   * Remove all previously added appenders from this Category instance.
   * 
   * <p>
   * This is useful when re-reading configuration information.
   */
  public void removeAllAppenders() {
    logger.removeAllAppenders();
  }

  /**
   * This is the most generic printing method. It is intended to be invoked by
   * <b>wrapper</b> classes.
   * 
   * @param callerFQCN
   *          The wrapper class' fully qualified class name.
   * @param level
   *          The level of the logging request.
   * @param message
   *          The message of the logging request.
   * @param t
   *          The throwable of the logging request, may be null.
   */
  public void log(String callerFQCN, Priority level, Object message, Throwable t) {
    logger.log(callerFQCN, level, message, t);
  }

  /**
   * @deprecated Please use {@link #getLevel} instead.
   */
  @Deprecated
  public Level getPriority() {
    return logger.getPriority();
  }

  /**
   * Returns all the currently defined categories in the default hierarchy as an
   * {@link Enumeration Enumeration}.
   * 
   * <p>
   * The root category is <em>not</em> included in the returned
   * {@link Enumeration}.
   * 
   * @deprecated Please use {@link LogManager#getCurrentLoggers()} instead.
   */
  @Deprecated
  public static Enumeration getCurrentCategories() {
    return Category.getCurrentCategories();
  }

  /**
   * Log a localized message. The user supplied parameter <code>key</code> is
   * replaced by its localized version from the resource bundle.
   * 
   * @see #setResourceBundle
   * @since 0.8.4
   * @param priority
   * @param key
   * @param t
   */
  public void l7dlog(Priority priority, String key, Throwable t) {
    logger.l7dlog(priority, key, t);
  }

  /**
   * Call the appenders in the hierrachy starting at <code>this</code>. If no
   * appenders could be found, emit a warning.
   * 
   * <p>
   * This method calls all the appenders inherited from the hierarchy
   * circumventing any evaluation of whether to log or not to log the particular
   * log request.
   * 
   * @param event
   *          the event to log.
   */
  public void callAppenders(LoggingEvent event) {
    logger.callAppenders(event);
  }

  /**
   * Set the resource bundle to be used with localized logging methods
   * {@link #l7dlog(Priority, String, Throwable)} and
   * {@link #l7dlog(Priority, String, Object[], Throwable)}.
   * 
   * @since 0.8.4
   * @param bundle
   */
  public void setResourceBundle(ResourceBundle bundle) {
    logger.setResourceBundle(bundle);
  }

  /**
   * Is the appender passed as parameter attached to this category?
   * 
   * @param appender
   */
  public boolean isAttached(Appender appender) {
    return logger.isAttached(appender);
  }

  /**
   * Check whether this category is enabled for the TRACE Level.
   * 
   * @since 1.2.12
   *
   * @return boolean - <code>true</code> if this category is enabled for level
   *         TRACE, <code>false</code> otherwise.
   */
  public boolean isTraceEnabled() {
    return logger.isTraceEnabled();
  }

  /**
   * Return the the {@link LoggerRepository} where this <code>Category</code> is
   * attached.
   * 
   * @since 1.2
   */
  public LoggerRepository getLoggerRepository() {
    return logger.getLoggerRepository();
  }

  /**
   * Check whether this category is enabled for a given {@link Level} passed as
   * parameter.
   * 
   * See also {@link #isDebugEnabled}.
   * 
   * @return boolean True if this category is enabled for <code>level</code>.
   * @param level
   */
  public boolean isEnabledFor(Priority level) {
    return logger.isEnabledFor(level);
  }

  /**
   * Return the the {@link Hierarchy} where this <code>Category</code> instance
   * is attached.
   * 
   * @deprecated Please use {@link #getLoggerRepository} instead.
   * @since 1.1
   */
  @Deprecated
  public LoggerRepository getHierarchy() {
    return logger.getHierarchy();
  }
}
