from langchain_community.document_loaders import WebBaseLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import AzureOpenAIEmbeddings
from langchain_community.vectorstores import FAISS
import os
from pathlib import Path
from dotenv import load_dotenv
import time
from bs4 import BeautifulSoup
import requests
#from dotenv import load_dotenv
load_dotenv()
os.environ['USER_AGENT'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'

VECTORSTORE_PATH = Path("athena_docs_vectorstore")
EMBEDDING_MODEL = "text-embedding-3-small"

ATHENA_DOC_URLS = [
    # AWS ATHENA CORE DOCUMENTATION
    "https://docs.aws.amazon.com/athena/latest/ug/ddl-sql-reference.html",
    "https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html",
    "https://docs.aws.amazon.com/athena/latest/ug/functions-env3.html",
    "https://docs.aws.amazon.com/athena/latest/ug/language-reference.html",
    "https://docs.aws.amazon.com/athena/latest/ug/select.html",
    "https://docs.aws.amazon.com/athena/latest/ug/reserved-words.html",
    "https://docs.aws.amazon.com/athena/latest/ug/data-types.html",
    "https://docs.aws.amazon.com/athena/latest/ug/compression-formats.html",
    "https://docs.aws.amazon.com/athena/latest/ug/geospatial-functions-list.html",
    "https://docs.aws.amazon.com/athena/latest/ug/querying-geospatial-data.html",
    "https://docs.aws.amazon.com/athena/latest/ug/geospatial-input-formats.html",
    "https://docs.aws.amazon.com/athena/latest/ug/rows-and-structs.html",
    "https://docs.aws.amazon.com/athena/latest/ug/querying-arrays.html",
    "https://docs.aws.amazon.com/athena/latest/ug/querying-JSON.html",
    "https://docs.aws.amazon.com/athena/latest/ug/creating-tables.html",
    "https://docs.aws.amazon.com/athena/latest/ug/alter-table.html",
    "https://docs.aws.amazon.com/athena/latest/ug/drop-table.html",
    "https://docs.aws.amazon.com/athena/latest/ug/ctas.html",
    "https://docs.aws.amazon.com/athena/latest/ug/views.html",
    "https://docs.aws.amazon.com/athena/latest/ug/partitions.html",
    "https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html",
    "https://docs.aws.amazon.com/athena/latest/ug/performance-tuning.html",
    "https://docs.aws.amazon.com/athena/latest/ug/query-optimization.html",
    "https://docs.aws.amazon.com/athena/latest/ug/querying.html",
    "https://docs.aws.amazon.com/athena/latest/ug/other-notable-limitations.html",
    
    # TRINO FUNCTIONS
    "https://trino.io/docs/current/functions.html",
    "https://trino.io/docs/current/functions/list.html",
    "https://trino.io/docs/current/functions/list-by-topic.html",
    "https://trino.io/docs/current/functions/aggregate.html",
    "https://trino.io/docs/current/functions/array.html",
    "https://trino.io/docs/current/functions/binary.html",
    "https://trino.io/docs/current/functions/bitwise.html",
    "https://trino.io/docs/current/functions/color.html",
    "https://trino.io/docs/current/functions/comparison.html",
    "https://trino.io/docs/current/functions/conditional.html",
    "https://trino.io/docs/current/functions/conversion.html",
    "https://trino.io/docs/current/functions/datetime.html",
    "https://trino.io/docs/current/functions/decimal.html",
    "https://trino.io/docs/current/functions/geospatial.html",
    "https://trino.io/docs/current/functions/hyperloglog.html",
    "https://trino.io/docs/current/functions/ipaddress.html",
    "https://trino.io/docs/current/functions/json.html",
    "https://trino.io/docs/current/functions/lambda.html",
    "https://trino.io/docs/current/functions/logical.html",
    "https://trino.io/docs/current/functions/map.html",
    "https://trino.io/docs/current/functions/math.html",
    "https://trino.io/docs/current/functions/ml.html",
    "https://trino.io/docs/current/functions/qdigest.html",
    "https://trino.io/docs/current/functions/regexp.html",
    "https://trino.io/docs/current/functions/session.html",
    "https://trino.io/docs/current/functions/setdigest.html",
    "https://trino.io/docs/current/functions/string.html",
    "https://trino.io/docs/current/functions/system.html",
    "https://trino.io/docs/current/functions/table.html",
    "https://trino.io/docs/current/functions/teradata.html",
    "https://trino.io/docs/current/functions/tdigest.html",
    "https://trino.io/docs/current/functions/url.html",
    "https://trino.io/docs/current/functions/uuid.html",
    "https://trino.io/docs/current/functions/window.html",
    
    # TRINO SQL SYNTAX
    "https://trino.io/docs/current/sql.html",
    "https://trino.io/docs/current/sql/select.html",
    "https://trino.io/docs/current/language/types.html",
    "https://trino.io/docs/current/language/reserved.html",
    "https://trino.io/docs/current/sql/create-table.html",
    "https://trino.io/docs/current/sql/create-table-as.html",
    "https://trino.io/docs/current/sql/drop-table.html",
    "https://trino.io/docs/current/sql/alter-table.html",
    "https://trino.io/docs/current/sql/create-view.html",
    "https://trino.io/docs/current/sql/drop-view.html",
    "https://trino.io/docs/current/sql/insert.html",
    "https://trino.io/docs/current/sql/delete.html",
    "https://trino.io/docs/current/sql/update.html",
    "https://trino.io/docs/current/sql/merge.html",
    "https://trino.io/docs/current/sql/explain.html",
    "https://trino.io/docs/current/sql/explain-analyze.html",
    "https://trino.io/docs/current/sql/show-functions.html",
    "https://trino.io/docs/current/sql/show-columns.html",
    "https://trino.io/docs/current/sql/show-tables.html",
    "https://trino.io/docs/current/sql/match-recognize.html",
]


def load_single_url_robust(url: str, retry_count: int = 3):
    """
    Robust URL loading with multiple strategies.
    Tries different content extraction methods.
    """
    for attempt in range(retry_count):
        try:
            # Strategy 1: Use requests directly for better control
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove unwanted elements
            for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside', 'iframe']):
                element.decompose()
            
            # Extract main content based on site
            content = None
            
            if 'trino.io' in url:
                # Trino: try multiple selectors
                content = (
                    soup.find('article') or 
                    soup.find('main') or 
                    soup.find('div', class_='content') or
                    soup.find('div', id='content')
                )
            
            elif 'amazonaws.com' in url:
                # AWS: try multiple selectors
                content = (
                    soup.find('div', id='main-content') or
                    soup.find('main') or
                    soup.find('article') or
                    soup.find('div', class_='awsui-util-container')
                )
            
            # Fallback: get body
            if content is None:
                content = soup.find('body')
            
            if content:
                text = content.get_text(separator='\n', strip=True)
                
                # Clean up excessive whitespace
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                text = '\n'.join(lines)
                
                # Create a Document-like object
                from langchain.schema import Document
                doc = Document(
                    page_content=text,
                    metadata={
                        'source': url,
                        'source_url': url,
                        'title': soup.title.string if soup.title else url
                    }
                )
                
                return doc
            
        except Exception as e:
            print(f"     Attempt {attempt + 1} failed: {str(e)[:100]}")
            if attempt < retry_count - 1:
                time.sleep(2)  # Wait before retry
            continue
    
    return None


def load_documentation():
    """Load all documentation with robust error handling."""
    print("=" * 80)
    print("LOADING ATHENA/TRINO DOCUMENTATION")
    print("=" * 80)
    
    all_docs = []
    failed_urls = []
    
    for idx, url in enumerate(ATHENA_DOC_URLS, 1):
        print(f"\n[{idx}/{len(ATHENA_DOC_URLS)}] {url}")
        
        doc = load_single_url_robust(url)
        
        if doc:
            all_docs.append(doc)
            print(f"   ✅ Loaded ({len(doc.page_content)} chars)")
        else:
            failed_urls.append(url)
            print(f"    Failed after retries")
        
        # Rate limiting
        time.sleep(0.5)
    
    print(f"\n{'='*80}")
    print(f"📊 SUMMARY:")
    print(f"  Successful: {len(all_docs)}/{len(ATHENA_DOC_URLS)}")
    print(f"   Failed: {len(failed_urls)}")
    print(f"{'='*80}")
    
    if failed_urls:
        print("\n Failed URLs (you can try manually later):")
        for url in failed_urls:
            print(f"  - {url}")
    
    return all_docs


def split_documents(docs):
    """Split documents into chunks optimized for function lookup."""
    print("\n" + "=" * 80)
    print("SPLITTING DOCUMENTS INTO CHUNKS")
    print("=" * 80)
    
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1400,         # Smaller chunks for precise function docsssss
        chunk_overlap=300,       # Good overlap for context
        separators=[
            "\n## ",             # Markdown h2
            "\n### ",            # Markdown h3
            "\n#### ",           # Markdown h4
            "\n\n",              # Paragraph breaks
            "\n",                # Line breaks
            ". ",                # Sentences
            " ",                 # Words
        ],
        length_function=len,
    )
    
    chunks = text_splitter.split_documents(docs)
    print(f" Split into {len(chunks)} chunks")
    
    # Show sample
    if chunks:
        print(f"\n📄 Sample chunk preview:")
        print(f"   {chunks[0].page_content[:200]}...")
    
    return chunks


def create_vectorstore(chunks):
    """Create FAISS vectorstore with progress tracking."""
    print("\n" + "=" * 80)
    print("CREATING VECTOR STORE")
    print("=" * 80)
    
    print("Initializing Azure OpenAI embeddings...")
    embeddings = AzureOpenAIEmbeddings(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        azure_deployment=EMBEDDING_MODEL,
        api_version=os.getenv("AZURE_OPENAI_API_VERSION")
    )
    
    print(f"Generating embeddings for {len(chunks)} chunks...")
    print("(This will take several minutes...)\n")
    
    # Process in batches to show progress
    batch_size = 50
    vectorstore = None
    
    for i in range(0, len(chunks), batch_size):
        batch = chunks[i:i+batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(chunks) + batch_size - 1) // batch_size
        
        print(f"  Processing batch {batch_num}/{total_batches} ({len(batch)} chunks)...", end='', flush=True)
        
        try:
            if vectorstore is None:
                vectorstore = FAISS.from_documents(batch, embeddings)
            else:
                batch_store = FAISS.from_documents(batch, embeddings)
                vectorstore.merge_from(batch_store)
            
            print(" ✅")
        except Exception as e:
            print(f"  Error: {str(e)[:50]}")
            continue
        
        # Small delay between batches
        time.sleep(1)
    
    print(f"\n Vector store created with {len(chunks)} chunks")
    
    return vectorstore


def save_vectorstore(vectorstore):
    """Save vectorstore to disk."""
    print("\n" + "=" * 80)
    print("SAVING VECTOR STORE")
    print("=" * 80)
    
    VECTORSTORE_PATH.mkdir(exist_ok=True)
    
    vectorstore.save_local(str(VECTORSTORE_PATH))
    
    # Verify files were created
    faiss_file = VECTORSTORE_PATH / "index.faiss"
    pkl_file = VECTORSTORE_PATH / "index.pkl"
    
    if faiss_file.exists() and pkl_file.exists():
        print(f" Vector store saved successfully!")
        print(f"    Location: {VECTORSTORE_PATH.absolute()}")
        print(f"    Files:")
        print(f"      - index.faiss ({faiss_file.stat().st_size / 1024:.1f} KB)")
        print(f"      - index.pkl ({pkl_file.stat().st_size / 1024:.1f} KB)")
    else:
        print(f"  Warning: Files may not have been created properly")


def main():
    """Main indexing workflow."""
    print("\n" + "=" * 80)
    print(" ATHENA/TRINO DOCUMENTATION INDEXER")
    print("=" * 80)
    print("\nThis script will:")
    print("  1. Load 79 documentation pages from AWS & Trino")
    print("  2. Extract and clean content")
    print("  3. Split into ~1200 char chunks")
    print("  4. Generate embeddings via Azure OpenAI")
    print("  5. Save FAISS vector store locally")
    print("\n  Estimated time: 10-15 minutes")
    print(f" Estimated cost: ~$0.05-0.10 (embeddings)")
    
    proceed = input("\n  Proceed? (yes/no): ").strip().lower()
    if proceed != 'yes':
        print(" Cancelled.")
        return
    
    try:
        # Step 1: Load documentation
        docs = load_documentation()
        if not docs:
            print("\n No documents loaded. Exiting.")
            return
        
        if len(docs) < 30:  # Less than 40% success rate
            print(f"\n  Warning: Only {len(docs)}/79 URLs loaded successfully.")
            cont = input("   Continue anyway? (yes/no): ").strip().lower()
            if cont != 'yes':
                print(" Cancelled.")
                return
        
        # Step 2: Split into chunks
        chunks = split_documents(docs)
        
        # Step 3: Create vectorstore
        vectorstore = create_vectorstore(chunks)
        
        # Step 4: Save
        save_vectorstore(vectorstore)
        
        print("\n" + "=" * 80)
        print(" INDEXING COMPLETE!")
        print("=" * 80)
        print(f"\n Successfully indexed {len(docs)} documentation pages")
        print(f" Created {len(chunks)} searchable chunks")
        print(f" Saved to: {VECTORSTORE_PATH.absolute()}")
        print("\n Next steps:")
        print("  1. Run your NL-to-SQL app")
        print("  2. SQL validation will use these docs automatically")
        print("  3. Re-run this script monthly to refresh")
        
    except KeyboardInterrupt:
        print("\n\n Interrupted by user")
    except Exception as e:
        print(f"\n Error during indexing: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()