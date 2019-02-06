package com.databasepreservation.utils;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.modules.DatabaseModuleFactory;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ReflectionUtils {
  private static Logger LOGGER = LoggerFactory.getLogger(ReflectionUtils.class);

  private static List<Constructor<? extends DatabaseModuleFactory>> databaseModuleFactoryConstructors = new ArrayList<>();

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
          Constructor<? extends DatabaseModuleFactory> constructor = moduleFactoryClass.getConstructor();
          databaseModuleFactoryConstructors.add(constructor);
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

}
