package io.delta.core;

import io.delta.core.data.ColumnarBatch;
import io.delta.core.utils.CloseableIterator;

public interface Scan {

    ////////////////////////////////////////////////////////////////////////////////////////////////
    //                                 APPROACH ONE                                               //
    //          Provide opaque serializable objects which provide an interface to                 //
    //          to get the data in the form of ColumnarBatches.                                   //
    ////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Get the list of tasks for this scan instance. The tasks are serializable.
     * @return
     */
    CloseableIterator<ScanTask> getTasks();


    ////////////////////////////////////////////////////////////////////////////////////////////////
    //                                 APPROACH TWO                                               //
    //       Provide the list of files to scan and the scan state in the form of                  //
    //       ColumnarBatches. The connector is free to serialize it in owns format and            //
    //       use the Delta Core api to get the data. The fetch data API takes one row from        //
    //       the survived file info and the only row from the scan state (common for all files)   //
    ////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Get an iterator of data files in this version of scan that survived the predicate pruning.
     *
     * @return data in {@link ColumnarBatch} batch format. Each row correspond to one survived file.
     */
    CloseableIterator<ColumnarBatch> survivedFileInfo();

    /**
     * Get the scan state associate with the current scan. This state is common to all survived
     * files.
     */
    CloseableIterator<ColumnarBatch> getScanState();
}
