/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"

//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	 // create index name
   std:: ostringstream idxStr;
   idxStr << relationName << '.' << attrByteOffset;
   std::string indexName = idxStr.str();
   std::cout << "Index name created: " << indexName << std::endl;


   outIndexName = indexName;
   bufMgr = bufMgrIn;
   attributeType = attrType;
   this->attrByteOffset = attrByteOffset;
   leafOccupancy = INTARRAYLEAFSIZE; //Do it for string and double
   nodeOccupancy = INTARRAYNONLEAFSIZE;
   scanExecuting = false;
   is_root_leaf = true; //when the index does not exist, the root will be leaf 


   // check if this index file already exists or not.
   if (!(File::exists(indexName))) {
      std::cout << "Index file does not exist" << std::endl;
      file = new BlobFile(indexName, true);


      //allocate new meta page
      Page* metaPage;
      bufMgr->allocPage(file, headerPageNum, metaPage); //can unpin these pages once info from them is retrieved?


      //allocate new root page      
      Page* rootPage;
      bufMgr->allocPage(file, rootPageNum, rootPage); 


      //populate IndexMetaInfo with rootPageNum
      IndexMetaInfo* metaInfo;
      metaInfo = (IndexMetaInfo*)metaPage;
      strcpy(metaInfo->relationName, relationName.c_str());
      metaInfo->attrByteOffset = attrByteOffset;
      metaInfo->attrType = attributeType;
      metaInfo->rootPageNo = rootPageNum;


      // for a new btree file, this should be a leaf node
      LeafNodeInt* root = reinterpret_cast< LeafNodeInt* >(rootPage); //should check the type and do this for string/double/integer
      root->rightSibPageNo = 0;      

      bufMgr->unPinPage(file, rootPageNum, true);
      bufMgr->unPinPage(file, headerPageNum, true);
      //scan records and insert into the Btree
      FileScan fscan(relationName, bufMgr);
      try
      {
        RecordId scanRid;
        while (1)
	{
	   //scannext
               fscan.scanNext(scanRid);
               std::string recordStr = fscan.getRecord();
               const char *record = recordStr.c_str();
               void* key = (void *)(record + attrByteOffset); //typecast should be specific to attrType - change this later while making changes for all types
	   //insertEntry
              insertEntry(key,scanRid);
	}
      }
      catch(EndOfFileException e)
      {
         std::cout << "Read relation and creating index file" << std::endl;	
      }
      bufMgr->flushFile(file);
      
   } else {
      std::cout << "Index file does exist" << std::endl;
      file = new BlobFile(indexName, false); //don't create a new blobfile in this case


      // read the first page from the file - which is the meta node
      headerPageNum = file->getFirstPageNo();


      //get the root page num from the meta node
      Page* metaPage;
      bufMgr->readPage(file, headerPageNum, metaPage); //unpin this page when done?
      IndexMetaInfo* metaInfo;
      metaInfo = (IndexMetaInfo*) metaPage;
      this->attrByteOffset = metaInfo->attrByteOffset;
      attributeType = metaInfo->attrType;
      rootPageNum = metaInfo->rootPageNo;
     		
      //read the root page (bufMgr->readPage(file, rootpageNum, out_root_page)
      Page* out_root_page;
      bufMgr->readPage(file, rootPageNum, out_root_page);
      //Root page starts as page 2 but since a split can occur
      //* at the root the root page may get moved up and get a new page no.
      // So, if a root page number is 2, it is a leaf node.
      if (rootPageNum == 2) {
	is_root_leaf = true;
      }
      bufMgr->unPinPage(file, headerPageNum, false);
      
   }
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    
    bufMgr->flushFile(file);
    delete file;
    scanExecuting = false;
    //what else necessary?
}

/*
 * Simple lookup function to find the pageid of the leaf where
 * a given rid,key pair is to be inserted.
*/
const void BTreeIndex::lookupLeaf(PageId currPageNo, RIDKeyPair<int> entry, PageKeyPair<int>& insertedPage)
{
	// Fetch current page for further traversal
	Page *currPage;
	bufMgr->readPage(file, currPageNo, currPage);


	// Convert current page to non leaf node format
	NonLeafNodeInt* currNode = reinterpret_cast<NonLeafNodeInt*> (currPage);


	// Find the index position in current node's page array of the next
	//  child page to be traversed.
	int idx = 0;
	for (idx = 0; (currNode->pageNoArray[idx] != 0) && (idx < nodeOccupancy); idx++) {
		if ((currNode->keyArray[idx] > entry.key))
			break;
	}

	// if the above loop terminated because valid pages ended,
	// decrease index by 1
	if (currNode->pageNoArray[idx] == 0)
		if (idx > 0)
	    	   idx--;

	// get the page number of the child
	PageId nextLevelPageNo = currNode->pageNoArray[idx];
	Page* nextLevelPage;

	PageKeyPair<int> newPage;

	// If current node is at level one, insert the entry at leaf
	// and return
	if (currNode->level == 1) {
		// If the leaf node is full, split it, else put the
		// entry in leaf
		bufMgr->readPage(file, nextLevelPageNo, nextLevelPage);
		LeafNodeInt* nextLevelLeafNode = reinterpret_cast<LeafNodeInt*>(nextLevelPage);
		if ((nextLevelLeafNode->ridArray[leafOccupancy-1]).page_number == 0) {
			insertEntryInLeaf(nextLevelLeafNode, entry);
		} else {
			// split the node and return the page info through newPage
			splitLeafNode(nextLevelLeafNode, entry, newPage);
			// insert page info in the current node
			if (currNode->pageNoArray[nodeOccupancy] == 0) {
				insertEntryInNonLeaf(currNode, newPage);
			}
		}
		bufMgr->unPinPage(file, nextLevelPageNo, true); 
		bufMgr->unPinPage(file, currPageNo, true);
		return;
	}

	//recursively traverse

	PageKeyPair<int> newInsertedPage;
	newInsertedPage.set(0,0);
	bufMgr->unPinPage(file,currPageNo,false); 
	lookupLeaf(nextLevelPageNo, entry, newInsertedPage);

	// do while traversing upwards
	Page* readThisPage;
	PageKeyPair<int> anotherNewPage;
	bufMgr->readPage(file,currPageNo,readThisPage);

	if (newInsertedPage.pageNo != 0) {
		newPage.set(newInsertedPage.pageNo, newInsertedPage.key);
		if (currNode->pageNoArray[nodeOccupancy]==0) {
			insertEntryInNonLeaf(currNode, newPage);
  		}  else {
  			splitNonLeafNode(currNode, newPage, anotherNewPage);
			insertedPage = anotherNewPage;
  		}
  	}
	int is_dirty;
	if (newInsertedPage.pageNo !=0)
		is_dirty = true;
	else
		is_dirty = false;
  	bufMgr->unPinPage(file, currPageNo, is_dirty); 
}

/*
 * Simple insert function to insert a rid,key pair in a given leaf.
 *
*/
const void BTreeIndex::insertLeafAtNode(RIDKeyPair<int> entry){
        Page* currentPage;
        bufMgr->readPage(file, rootPageNum, currentPage);
        PageId prevRoot = rootPageNum;
        LeafNodeInt* leafNode = reinterpret_cast<LeafNodeInt*> (currentPage);
        int is_root_leaf_full = (leafNode->ridArray[leafOccupancy-1].page_number != 0);

        // Simple insert if the required leaf is not already full.
        if(!is_root_leaf_full){
                //leafNode->keyArray[index] = *((int*) key);
                //leafNode->ridArray[index] = rid;
                insertEntryInLeaf(leafNode, entry);
        }
        // Split and insert called if the required leaf is full.
        else{
                PageKeyPair<int> newPage;
                splitLeafNode(leafNode, entry, newPage);
                makeNewRootNode(rootPageNum, newPage, true);
        }

        bufMgr->unPinPage(file, prevRoot, true);
}


// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	if(is_root_leaf){
		// Insert only if the index is Integer type
		if (attributeType != INTEGER){
			std::cout << "NON INTEGER TYPE INDEX NOT SUPPORTED.";
		} else{
			RIDKeyPair<int> entry;
			entry.set(rid, *((int*)(key)));
			BTreeIndex::insertLeafAtNode(entry);
		}
	} else{
		if (attributeType != INTEGER){
			std::cout << "NON INTEGER TYPE INDEX NOT SUPPORTED.";
		} else{
			RIDKeyPair<int> entry;
			entry.set(rid, *((int*)(key)));
			PageKeyPair<int> insertedPage;
			insertedPage.set(0,entry.key);
			BTreeIndex::lookupLeaf(rootPageNum, entry, insertedPage);
			//create a new root if needed
			PageId prevRoot = rootPageNum;
			Page* rootPage;
			bufMgr->readPage(file, prevRoot, rootPage);
			if (insertedPage.pageNo != 0) {
				makeNewRootNode(prevRoot, insertedPage, false);
			}
			bufMgr->unPinPage(file, prevRoot, true);
		}
	}

}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntryInLeaf
// insert entry inleaf 
// ----------------------------------------------------------------------------
void BTreeIndex::insertEntryInLeaf(LeafNodeInt* leafNode, RIDKeyPair<int> entry){
    //find the position in the leaf node for the entry
    int idx;
    for (idx = 0; idx < leafOccupancy && (leafNode->ridArray[idx].page_number != 0); idx++) {
      if (leafNode->keyArray[idx] >= entry.key)
        break;	
    }

    // now the position to insert is found, shift the entries to the right
    for (int i = leafOccupancy - 1; i > idx; i--){
      leafNode->ridArray[i] = leafNode->ridArray[i-1];
      leafNode->keyArray[i] = leafNode->keyArray[i-1];      
    }

    // insert the entry at right position
    leafNode->ridArray[idx] = entry.rid;
    leafNode->keyArray[idx] = entry.key;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntryInNonLeaf
// insert entry in non leaf
// ----------------------------------------------------------------------------
void BTreeIndex::insertEntryInNonLeaf(NonLeafNodeInt* nonLeafNode, PageKeyPair<int> entry){
    //find the position in the leaf node for the entry
    int idx;
    for (idx = 0; ((nonLeafNode->pageNoArray[idx]) != 0 && (idx < leafOccupancy)); idx++) {
      if (nonLeafNode->keyArray[idx] >= entry.key)
        break;
    }

    // shift the entries to the right 
    for (int i = leafOccupancy - 1; i > idx; i--){
      nonLeafNode->keyArray[i] = nonLeafNode->keyArray[i-1];
      nonLeafNode->pageNoArray[i+1] = nonLeafNode->pageNoArray[i];
    }

    // insert the entry in either last or on the position found
    if (nonLeafNode->pageNoArray[idx] == 0) {
        nonLeafNode->pageNoArray[idx] = entry.pageNo;
	nonLeafNode->keyArray[idx-1] = entry.key;
    } else {
	nonLeafNode->pageNoArray[idx+1] = entry.pageNo;
	nonLeafNode->keyArray[idx] = entry.key;
    }
}

const void BTreeIndex::makeNewRootNode(PageId pid, PageKeyPair<int> pageKey, bool setlevel){

	// allocate a new page
	Page* newRootPage;
	PageId newRootPageNo;
	bufMgr->allocPage(file, newRootPageNo, newRootPage);

	// set values in the new node
	NonLeafNodeInt* newRootNode = (NonLeafNodeInt*)newRootPage;
	(setlevel) ? newRootNode->level = 1: newRootNode->level = 0;
	newRootNode->pageNoArray[0] = pid;
	newRootNode->pageNoArray[1] = pageKey.pageNo;
	newRootNode->keyArray[0] = pageKey.key;

	// make changes to root page info and metapage
	rootPageNum = newRootPageNo;
	is_root_leaf = false;
	Page* headerPage;
	bufMgr->readPage(file, headerPageNum, headerPage);
	IndexMetaInfo * metaPage;
	metaPage = (IndexMetaInfo*) headerPage;
	metaPage->rootPageNo = rootPageNum;

	bufMgr->unPinPage(file, newRootPageNo, true);
	bufMgr->unPinPage(file, headerPageNum, true);

}

/*
 * Split and insert function to be called if
 * the required leaf was determined to be full.
 *
*/
const void BTreeIndex::splitLeafNode(LeafNodeInt* leafNode, RIDKeyPair<int> entry, PageKeyPair<int>& newPage) {
    //allocate new page
    PageId PageNo;
    Page* Page;
    bufMgr->allocPage(file, PageNo, Page);

    // cast it as new leaf node
    LeafNodeInt* newNode = reinterpret_cast<LeafNodeInt*>(Page);	

    // get the mid pint
    int mid = leafOccupancy/2+1;

    // correct the sibling info
    newNode->rightSibPageNo = leafNode->rightSibPageNo;
    leafNode->rightSibPageNo = PageNo;

    // assign proper values
    for (int i = mid; i < leafOccupancy; i++) {
	newNode->ridArray[i-mid] = leafNode->ridArray[i];
        newNode->keyArray[i-mid] = leafNode->keyArray[i];
	leafNode->ridArray[i].page_number = 0;
    }

    //decide on which leaf node to insert the entry and insert
    if (entry.key < newNode->keyArray[0]) {
	insertEntryInLeaf(leafNode, entry);
    } else {
	insertEntryInLeaf(newNode,entry);
    }

    // set entry for return
    newPage.set(PageNo, newNode->keyArray[0]);

    bufMgr->unPinPage(file, PageNo, true); 
}

/*
 * Split and insert function to be called if
 * the required non leaf was determined to be full.
 *
*/
const void BTreeIndex::splitNonLeafNode(NonLeafNodeInt* nonLeafNode, PageKeyPair<int> entry, PageKeyPair<int>& newInsertedPage) {
    //allocate new page
    PageId newPageNo;
    Page* newPage;
    bufMgr->allocPage(file, newPageNo, newPage);

    // cast it as new nonleaf node
    NonLeafNodeInt* newNode = reinterpret_cast<NonLeafNodeInt*>(newPage);	

    // find mid point
    int mid = nodeOccupancy/2+1;

    // set the level
    newNode->level = nonLeafNode->level; 

    // assign values for separation
    for (int i = mid; i < nodeOccupancy; i++) {
	newNode->keyArray[i-mid] = nonLeafNode->keyArray[i];
	newNode->pageNoArray[i-mid] = nonLeafNode->pageNoArray[i];
    }
    // set page array correctly
    newNode->pageNoArray[nodeOccupancy-mid] = nonLeafNode->pageNoArray[nodeOccupancy];
    nonLeafNode->pageNoArray[nodeOccupancy] = 0;

    // decide where to insert	
    if (entry.key < newNode->keyArray[0]){
	insertEntryInNonLeaf(nonLeafNode, entry);
    } else{
	insertEntryInNonLeaf(newNode, entry);
    }

    // set the values for return
    newInsertedPage.set(newPageNo,newNode->keyArray[0]);
    bufMgr->unPinPage(file, newPageNo, true);
}

const void BTreeIndex::findStartRecordID(Page *rootPage){
        NonLeafNodeInt *node;
        node = (NonLeafNodeInt*)rootPage;
        if(node->level == 0) // Leaf node
        {
                int i = 0;
                while(i < INTARRAYLEAFSIZE-1){
                        if(lowOp == GT && lowValInt > node->keyArray[i]){
                                // Pin page in the buffer pool
                                currentPageNum = node->pageNoArray[i];
                                bufMgr->allocPage(file, currentPageNum, currentPageData);
                                nextEntry = i;
                                return;
                        }
                        else if(lowOp == GTE && lowValInt >= node->keyArray[i]){
                                  // Pin page in the buffer pool
                                  // set the current page number
                                  // Set the current page and pin it 
                                currentPageNum = node->pageNoArray[i];
                                bufMgr->allocPage(file, currentPageNum, currentPageData);
                                nextEntry = i;
                                return;
                      }
                        i++;
                }
                // No node was found // throw exception
                throw NoSuchKeyFoundException();
        }
        else{
                // Non - leaf node
                int i = 0;
                while(lowOp == GT && i < INTARRAYNONLEAFSIZE-1 && lowValInt > node->keyArray[i]){
                                i++;
                        }
                while(lowOp == GTE && i < INTARRAYNONLEAFSIZE-1 && lowValInt >= node->keyArray[i]){
                                i++;
                }
                Page* page;
// This may not always be in the buffer 
                bufMgr->readPage(file, node->pageNoArray[i], page);
                findStartRecordID(page);
        }
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	// Check for errors
        if(scanExecuting == true)
                endScan();
        scanExecuting = true;
        if(lowOpParm != GT || lowOpParm != GTE)
                throw BadOpcodesException();
        if(highOpParm != LT || highOpParm != LTE)
                throw BadOpcodesException();
        if(highValParm < lowValParm)
                throw BadScanrangeException();
        if(attributeType == 0)// Integer keys
        {
                lowValInt = *(int*)lowValParm;
                highValInt = *(int*) highValParm;
        }
        else if(attributeType == 1){
                lowValDouble = *(double*)lowValParm;
                highValDouble = *(double*)highValParm;
        }
        else if(attributeType == 2)
        {
                lowValString = *(char*)lowValParm;
                highValString = *(char*) highValParm;
        }

        lowOp = lowOpParm;
        highOp = highOpParm;
        // Do we need to do this for everydata type?
        Page *rootPage;
        bufMgr->readPage(file, rootPageNum, rootPage); // read the root node 

        // Linear search on the keys array to find the page to traverse to next page
        findStartRecordID(rootPage);
        // Find the first page set up the value
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{
	if(scanExecuting == false)
                throw ScanNotInitializedException();

        LeafNodeInt *currentPage = (LeafNodeInt*) currentPageData;

        if(highOp == LT  && currentPage->keyArray[nextEntry] <  highValInt ){ // Satisfies the condition 
                outRid = currentPage->ridArray[nextEntry];
                nextEntry++;
        }
        else if(highOp == LTE && currentPage->keyArray[nextEntry] <= highValInt ){
                outRid = currentPage->ridArray[nextEntry];
                nextEntry++;
        }
        else{
                // No More records found 
                throw IndexScanCompletedException();
        }
        if(nextEntry >= INTARRAYLEAFSIZE) // Move to the next page if exists
        {
                // Unpin the older page 
                bufMgr->unPinPage(file, currentPageNum, false); // Assuming that once the records are generated we just read from them 
                currentPageNum = currentPage->rightSibPageNo;
                bufMgr->allocPage(file, currentPageNum, currentPageData);
        }
}



// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
	/ If no scan is initialized 
        if(!scanExecuting){
                throw ScanNotInitializedException();
        }
        else{
                // A scan is currently executing 
                // Reset all the scan variables
                //Unpin pinned pages
                bufMgr->unPinPage(file, currentPageNum, false);
                scanExecuting = false;
                nextEntry = -1;
                currentPageNum = -1;
                //lowOp = NULL; // how to deal with these operators ?
                //highOp = NULL;
                // the high and lower values must be reset
        }

}

}

}


