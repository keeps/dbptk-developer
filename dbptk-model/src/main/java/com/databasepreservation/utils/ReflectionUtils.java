/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.utils;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.modules.edits.EditModuleFactory;
import com.databasepreservation.model.modules.filters.DatabaseFilterFactory;
import com.databasepreservation.model.modules.validate.ValidateModuleFactory;
import com.databasepreservation.model.modules.validate.components.ValidatorComponentFactory;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ReflectionUtils {
  private static Logger LOGGER = LoggerFactory.getLogger(ReflectionUtils.class);

  private static List<Constructor<? extends DatabaseModuleFactory>> databaseModuleFactoryConstructors = new ArrayList<>();
  private static List<Constructor<? extends DatabaseFilterFactory>> databaseFilterFactoryConstructors = new ArrayList<>();
  private static List<Constructor<? extends EditModuleFactory>> editModuleFactoryConstructors         = new ArrayList<>();
  private static List<Constructor<? extends ValidateModuleFactory>> validateModuleFactoryConstructors = new ArrayList<>();
  private static List<Constructor<? extends ValidatorComponentFactory>> validatorComponentFactoryConstructors = new ArrayList<>();

  public static Set<DatabaseModuleFactory> collectDatabaseModuleFactories() {
    return collectDatabaseModuleFactories(false);
  }

  public static Set<DatabaseModuleFactory> collectDatabaseModuleFactories(boolean includeDisabled) {
    Set<DatabaseModuleFactory> databaseModuleFactories = new HashSet<>();

    if (databaseModuleFactoryConstructors.isEmpty()) {
      Reflections reflections = new Reflections("com.databasepreservation.modules", new SubTypesScanner());

      Set<Class<? extends DatabaseModuleFactory>> moduleFactoryClasses = reflections
        .getSubTypesOf(DatabaseModuleFactory.class);
      for (Class<? extends DatabaseModuleFactory> moduleFactoryClass : moduleFactoryClasses) {
        try {
            if (!java.lang.reflect.Modifier.isAbstract(moduleFactoryClass.getModifiers())) {
              Constructor<? extends DatabaseModuleFactory> constructor = moduleFactoryClass.getConstructor();
              databaseModuleFactoryConstructors.add(constructor);
            }
        } catch (NoSuchMethodException e) {
          LOGGER.info("Module factory {} could not be loaded", moduleFactoryClass.getName(), e);
        }
      }
    }

    for (Constructor<? extends DatabaseModuleFactory> constructor : databaseModuleFactoryConstructors) {
      try {
        DatabaseModuleFactory instance = constructor.newInstance();
        if (includeDisabled || instance.isEnabled()) {
          databaseModuleFactories.add(instance);
        }
      } catch (Exception e) {
        LOGGER.info("Module factory {} could not be loaded", constructor.getDeclaringClass().getName(), e);
      }
    }

    return databaseModuleFactories;
  }

  public static List<DatabaseFilterFactory> collectDatabaseFilterFactory() {
    return collectDatabaseFilterFactory(false);
  }

  public static List<DatabaseFilterFactory> collectDatabaseFilterFactory(boolean includeDisabled) {
    List<DatabaseFilterFactory> databaseFilterFactories = new ArrayList<>();

    if (databaseFilterFactoryConstructors.isEmpty()) {
      Reflections reflections = new Reflections("com.databasepreservation.modules", new SubTypesScanner());

      Set<Class<? extends DatabaseFilterFactory>> filterFactoryClasses = reflections
              .getSubTypesOf(DatabaseFilterFactory.class);

      for (Class<? extends DatabaseFilterFactory> filterFactoryClass : filterFactoryClasses) {
        try {
          Constructor<? extends DatabaseFilterFactory> constructor = filterFactoryClass.getConstructor();
          databaseFilterFactoryConstructors.add(constructor);
        } catch (NoSuchMethodException e) {
          LOGGER.info("Filter factory {} could not be loaded", filterFactoryClass.getName(), e);
        }
      }
    }

    for (Constructor<? extends DatabaseFilterFactory> constructor : databaseFilterFactoryConstructors) {
      try {
        DatabaseFilterFactory instance = constructor.newInstance();
        if (includeDisabled || instance.isEnabled()) {
          databaseFilterFactories.add(instance);
        }
      } catch (Exception e) {
        LOGGER.info("Filter factory {} could not be loaded", constructor.getDeclaringClass().getName(), e);
      }
    }

    return databaseFilterFactories;
  }

  public static Set<EditModuleFactory> collectEditModuleFactories() {
    return collectEditModuleFactories(false);
  }

  public static Set<EditModuleFactory> collectEditModuleFactories(boolean includeDisabled) {
    Set<EditModuleFactory> editModuleFactories = new HashSet<>();

    if (editModuleFactoryConstructors.isEmpty()) {
      Reflections reflections = new Reflections("com.databasepreservation.modules", new SubTypesScanner());

      Set<Class<? extends EditModuleFactory>> editFactoryClasses = reflections.getSubTypesOf(EditModuleFactory.class);

      for (Class<? extends EditModuleFactory> editModuleClass : editFactoryClasses) {
        try {
          Constructor<? extends EditModuleFactory> constructor = editModuleClass.getConstructor();
          editModuleFactoryConstructors.add(constructor);
        } catch (NoSuchMethodException e) {
          LOGGER.info("Filter factory {} could not be loaded", editModuleClass.getName(), e);
        }
      }
    }

    for (Constructor<? extends EditModuleFactory> constructor : editModuleFactoryConstructors) {
      try {
        EditModuleFactory instance = constructor.newInstance();
        if (includeDisabled || instance.isEnabled()) {
          editModuleFactories.add(instance);
        }
      } catch (Exception e) {
        LOGGER.info("Filter factory {} could not be loaded", constructor.getDeclaringClass().getName(), e);
      }
    }

    return editModuleFactories;
  }

  public static Collection<ValidateModuleFactory> collectValidateModuleFactories() {
    return collectValidateModuleFactories(false);
  }

  public static Collection<ValidateModuleFactory> collectValidateModuleFactories(boolean includeDisabled) {
    Set<ValidateModuleFactory> validateModuleFactories = new HashSet<>();

    if (validateModuleFactoryConstructors.isEmpty()) {
      Reflections reflections = new Reflections("com.databasepreservation.modules", new SubTypesScanner());

      Set<Class<? extends ValidateModuleFactory>> validateFactoryClasses = reflections.getSubTypesOf(ValidateModuleFactory.class);

      for (Class<? extends ValidateModuleFactory> validateModuleClass : validateFactoryClasses) {
        try {
          Constructor<? extends ValidateModuleFactory> constructor = validateModuleClass.getConstructor();
          validateModuleFactoryConstructors.add(constructor);
        } catch (NoSuchMethodException e) {
          LOGGER.info("Filter factory {} could not be loaded", validateModuleClass.getName(), e);
        }
      }
    }

    for (Constructor<? extends ValidateModuleFactory> constructor : validateModuleFactoryConstructors) {
      try {
        ValidateModuleFactory instance = constructor.newInstance();
        if (includeDisabled || instance.isEnabled()) {
          validateModuleFactories.add(instance);
        }
      } catch (Exception e) {
        LOGGER.info("Filter factory {} could not be loaded", constructor.getDeclaringClass().getName(), e);
      }
    }

    return validateModuleFactories;
  }

  public static Collection<ValidatorComponentFactory> collectValidatorComponentFactories() {
    return collectValidatorComponentFactories(false);
  }

  public static Collection<ValidatorComponentFactory> collectValidatorComponentFactories(boolean includeDisabled) {
    Set<ValidatorComponentFactory> validatorComponentFactories = new HashSet<>();

    if (validatorComponentFactoryConstructors.isEmpty()) {
      Reflections reflections = new Reflections("com.databasepreservation.modules.siard.validate",
          new SubTypesScanner());

      Set<Class<? extends ValidatorComponentFactory>> validatorComponentFactoryClasses = reflections
          .getSubTypesOf(ValidatorComponentFactory.class);

      for (Class<? extends ValidatorComponentFactory> aClass : validatorComponentFactoryClasses) {
        try {
          Constructor<? extends ValidatorComponentFactory> constructor = aClass.getConstructor();
          validatorComponentFactoryConstructors.add(constructor);
        } catch (NoSuchMethodException e) {
          LOGGER.info("Filter factory {} could not be loaded", aClass.getName(), e);
        }
      }
    }

    for (Constructor<? extends ValidatorComponentFactory> constructor : validatorComponentFactoryConstructors) {
      try {
        ValidatorComponentFactory instance = constructor.newInstance();
        if (includeDisabled || instance.isEnabled()) {
          validatorComponentFactories.add(instance);
        }
      } catch (Exception e) {
        LOGGER.info("Filter factory {} could not be loaded", constructor.getDeclaringClass().getName(), e);
      }
    }

    return validatorComponentFactories;
  }
}
