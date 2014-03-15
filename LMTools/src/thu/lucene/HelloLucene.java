package thu.lucene;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

public class HelloLucene {

	private static final String FIELDNAME = "fieldname";

	public static void main(String[] args) {
		@SuppressWarnings("deprecation")
		Version version = Version.LUCENE_CURRENT;
		Analyzer analyzer = new StandardAnalyzer(version);
		Directory directory = new RAMDirectory();
		IndexWriterConfig config = new IndexWriterConfig(version, analyzer);
		try {
			IndexWriter writer = new IndexWriter(directory, config);
			Document document = new Document();
			String text = "This is the text to be indexed.";
			document.add(new Field(FIELDNAME, text, TextField.TYPE_STORED));
			writer.addDocument(document);
			writer.close();
			DirectoryReader reader = DirectoryReader.open(directory);
			IndexSearcher searcher = new IndexSearcher(reader);
			QueryParser parser = new QueryParser(version,
					HelloLucene.FIELDNAME, analyzer);
			Query query = parser.parse("text");
			ScoreDoc[] hitsDocs = searcher.search(query, null, 1000).scoreDocs;
			System.out.println(hitsDocs.length);
			for (int i = 0; i < hitsDocs.length; i++) {
				Document hitsDoc = searcher.doc(hitsDocs[i].doc);
				System.out.println(hitsDoc.get(FIELDNAME));
			}
			reader.close();
			directory.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
