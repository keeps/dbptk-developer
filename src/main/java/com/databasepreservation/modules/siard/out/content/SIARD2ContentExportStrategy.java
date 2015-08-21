package com.databasepreservation.modules.siard.out.content;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ContentExportStrategy implements ContentExportStrategy {
        private final ContentPathExportStrategy contentPathStrategy;
        private final WriteStrategy writeStrategy;
        private final SIARDArchiveContainer baseContainer;
        private final boolean prettyXMLOutput;

        public SIARD2ContentExportStrategy(ContentPathExportStrategy contentPathStrategy, WriteStrategy writeStrategy,
          SIARDArchiveContainer baseContainer, boolean prettyXMLOutput) {
                this.contentPathStrategy = contentPathStrategy;
                this.writeStrategy = writeStrategy;
                this.baseContainer = baseContainer;

                this.prettyXMLOutput = prettyXMLOutput;
        }

        @Override public void openSchema(SchemaStructure schema) throws ModuleException {

        }

        @Override public void closeSchema(SchemaStructure schema) throws ModuleException {

        }

        @Override public void openTable(TableStructure table) throws ModuleException {

        }

        @Override public void closeTable(TableStructure table) throws ModuleException {

        }

        @Override public void tableRow(Row row) throws ModuleException {

        }
}
