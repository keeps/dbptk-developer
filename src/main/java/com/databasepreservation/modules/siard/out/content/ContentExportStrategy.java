package com.databasepreservation.modules.siard.out.content;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ContentExportStrategy {
        void openSchema(SchemaStructure schema) throws ModuleException;

        void closeSchema(SchemaStructure schema) throws ModuleException;

        void openTable(TableStructure table) throws ModuleException;

        void closeTable(TableStructure table) throws ModuleException;

        void tableRow(Row row) throws ModuleException;
}
