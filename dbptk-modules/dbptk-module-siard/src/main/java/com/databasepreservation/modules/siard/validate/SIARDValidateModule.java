/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.common.ValidationObserver;
import com.databasepreservation.common.ValidatorPathStrategy;
import com.databasepreservation.common.ZipFileManagerStrategy;
import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.SIARDVersionNotSupportedException;
import com.databasepreservation.model.modules.validate.ValidateModule;
import com.databasepreservation.model.modules.validate.components.ValidatorComponent;
import com.databasepreservation.model.modules.validate.components.ValidatorComponentFactory;
import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.modules.DefaultExceptionNormalizer;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import com.databasepreservation.modules.siard.in.read.ZipAndFolderReadStrategy;
import com.databasepreservation.modules.siard.validate.common.ZipFileManager;
import com.databasepreservation.modules.siard.validate.common.path.ValidatorPathStrategyImpl;
import com.databasepreservation.utils.ReflectionUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDValidateModule implements ValidateModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDValidateModule.class);
  private Reporter reporter;
  private ValidationObserver observer;
  private final Path SIARDPackageNormalizedPath;
  private ValidationReporter validationReporter;
  private final List<String> allowedUDTs;
  private final ValidatorPathStrategy validatorPathStrategy;
  private final ZipFileManagerStrategy zipFileManager;

  /**
   * Constructor used to initialize required objects to get an validate module for
   * SIARD 2 (all minor versions)
   *
   * @param SIARDPackagePath
   *          Path to the main SIARD file (file with extension .siard)
   */
  public SIARDValidateModule(Path SIARDPackagePath, Path validationReporterPath) {
    SIARDPackageNormalizedPath = SIARDPackagePath.toAbsolutePath().normalize();
    validationReporter = new ValidationReporter(validationReporterPath.toAbsolutePath().normalize(),
      SIARDPackageNormalizedPath);
    allowedUDTs = Collections.emptyList();
    validatorPathStrategy = new ValidatorPathStrategyImpl();
    zipFileManager = new ZipFileManager();
  }

  /**
   * Constructor used to initialize required objects to get an validate module for
   * SIARD 2 (all minor versions)
   *
   * @param SIARDPackagePath
   *          Path to the main SIARD file (file with extension .siard)
   */
  public SIARDValidateModule(Path SIARDPackagePath, Path validationReporterPath, Path allowedUDTs) {
    SIARDPackageNormalizedPath = SIARDPackagePath.toAbsolutePath().normalize();
    validationReporter = new ValidationReporter(validationReporterPath.toAbsolutePath().normalize(),
      SIARDPackageNormalizedPath);
    this.allowedUDTs = parseAllowUDTs(allowedUDTs);
    validatorPathStrategy = new ValidatorPathStrategyImpl();
    zipFileManager = new ZipFileManager();
  }

  /**
   * The reporter is set specifically for each module
   *
   * @param reporter
   *          The reporter that should be used by this ValidateModule
   */
  @Override
  public void setOnceReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  @Override
  public void setObserver(ValidationObserver observer) {
    this.observer = observer;
  }

  /**
   * @throws ModuleException
   *           Generic module exception
   */
  @Override
  public boolean validate() throws ModuleException {

    if (!validateSIARDVersion()) {
      throw new SIARDVersionNotSupportedException().withVersionInfo("SIARD 2.1 version").withMessage("SIARD validator only supports");
    }

    List<ValidatorComponent> components = getValidationComponents();
    int counter = 0;

    for (ValidatorComponent component : components) {
      component.setReporter(reporter);
      component.setObserver(observer);
      component.setZipFileManager(zipFileManager);
      component.setSIARDPath(SIARDPackageNormalizedPath);
      component.setValidationReporter(validationReporter);
      component.setValidatorPathStrategy(validatorPathStrategy);
      component.setAllowedUTD(allowedUDTs);
      component.setup();
      final boolean validate = component.validate();
      if (!validate) {
        counter++;
      }
      component.clean();
    }

    boolean result = counter == 0;
    observer.notifyIndicators(validationReporter.getNumberOfPassed(), validationReporter.getNumberOfErrors(), validationReporter.getNumberOfWarnings() , validationReporter.getNumberOfSkipped());
    observer.notifyValidationProcessFinish(result);

    validationReporter.close();
    
    return result;
  }

  /**
   * Normalize the exception into a ModuleException that is easier to understand
   * and handle.
   *
   * @param exception
   *          The Exception that would otherwise be thrown
   * @param contextMessage
   * @return A normalized exception using ModuleException or one of its subclasses
   */
  @Override
  public ModuleException normalizeException(Exception exception, String contextMessage) {
    return DefaultExceptionNormalizer.getInstance().normalizeException(exception, contextMessage);
  }

  private List<String> parseAllowUDTs(Path path) {
    if (path.toFile().exists() && path.toFile().isFile()) {
      List<String> lines = Collections.emptyList();
      try {
        lines = Files.readAllLines(path, StandardCharsets.UTF_8);
      } catch (IOException e) {
        e.printStackTrace();
      }

      return lines;
    }

    return Collections.emptyList();
  }

  private List<ValidatorComponent> getValidationComponents() throws ModuleException {
    List<ValidatorComponent> components = new ArrayList<>();

    final Collection<ValidatorComponentFactory> validatorComponentFactories = ReflectionUtils
      .collectValidatorComponentFactories(false);

    List<ValidatorComponentFactory> order = new ArrayList<>();
    ValidatorComponentFactory next = null;
    for (ValidatorComponentFactory factory : validatorComponentFactories) {
      if (factory.isFirst()) {
        order.add(factory);
        next = getFactory(validatorComponentFactories, factory.next());
      }
    }

    while (order.size() < validatorComponentFactories.size()) {
      order.add(next);
      if (next != null) {
        next = getFactory(validatorComponentFactories, next.next());
      }
    }

    for (ValidatorComponentFactory factory : order) {
      components.add(factory.buildComponent(reporter));
    }

    return components;
  }

  private ValidatorComponentFactory getFactory(Collection<ValidatorComponentFactory> factories, String componentName) {
    for (ValidatorComponentFactory factory : factories) {
      if (factory.getComponentName().equals(componentName)) {
        return factory;
      }
    }
    return null;
  }

  private boolean validateSIARDVersion() throws ModuleException {
    SIARDArchiveContainer mainContainer = new SIARDArchiveContainer(SIARDPackageNormalizedPath,
      SIARDArchiveContainer.OutputContainerType.MAIN);
    ReadStrategy readStrategy = new ZipAndFolderReadStrategy(mainContainer);

    readStrategy.setup(mainContainer);

    if (mainContainer.getVersion() == null) return false;
    readStrategy.finish(mainContainer);
    return mainContainer.getVersion().equals(SIARDConstants.SiardVersion.V2_1);
  }
}
