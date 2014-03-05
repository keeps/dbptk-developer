package pt.gov.dgarq.roda.common.convert.db.viewer;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;


/**
 * 
 * @author Miguel Coutada
 *
 */

public class Viewer {
		
//		private static final Logger logger = Logger.getLogger(Viewer.class);
//	
//		private static final Version LUCENE_VERSION = Version.LUCENE_47;
//
//		private Directory directory;
//		
//		private IndexWriterConfig config;
//		
//		private IndexWriter writer;
//		
//		public Viewer() {
//			directory = new RAMDirectory();
//			config = new IndexWriterConfig(LUCENE_VERSION, 
//					new SimpleAnalyzer(LUCENE_VERSION));
//			init();
//		}
//		
//		protected void init() {
//			try {
//				writer = new IndexWriter(directory, config);
//			} catch (IOException e) {
//				logger.debug("An error ocurred while creating Index Writer");
//				e.printStackTrace();
//			}
//		}
//		
//		private void addDoc(IndexWriter w, String title) throws IOException {
//			Document doc = new Document();
//			doc.add(new TextField("title", title, Field.Store.YES));
//			w.addDocument(doc);
//		}
//		
//		
//
//		public IndexWriter getWriter() {
//			return writer;
//		}
//
//		public void setWriter(IndexWriter writer) {
//			this.writer = writer;
//		}
		
//	public static void main(String[] args) throws IOException {
//		               RAMDirectory directory = new RAMDirectory();
//		               IndexWriter writer = 
//		                 new IndexWriter(directory, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);
//		                   
//		               Document doc = new Document(); 
//		               doc.add(new Field("partnum", "Q36", Field.Store.YES, Field.Index.NOT_ANALYZED));   
//		               doc.add(new Field("description", "Illidium Space Modulator", Field.Store.YES, Field.Index.ANALYZED)); 
//		               writer.addDocument(doc); 
//		               writer.close();
//		   
//		               IndexSearcher searcher = new IndexSearcher(directory);
//		               Query query = new TermQuery(new Term("partnum", "Q36"));   
//		               TopDocs rs = searcher.search(query, null, 10);
//		               System.out.println(rs.totalHits);
//		   
//		               Document firstHit = searcher.doc(rs.scoreDocs[0].doc);
//		               System.out.println(firstHit.getField("partnum").name());
//		           }
//		
}
