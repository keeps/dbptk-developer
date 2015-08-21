package com.databasepreservation.modules.siard.in.content;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.DatabaseHandler;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ContentImportStrategy implements ContentImportStrategy {
        private final ContentPathImportStrategy contentPathStrategy;
        private final ReadStrategy readStrategy;

        public SIARD2ContentImportStrategy(ReadStrategy readStrategy, ContentPathImportStrategy contentPathStrategy) {
                this.contentPathStrategy = contentPathStrategy;
                this.readStrategy = readStrategy;
        }

        @Override public void importContent(DatabaseHandler handler, SIARDArchiveContainer container,
          DatabaseStructure databaseStructure) throws ModuleException {

        }
}
