package com.ociweb.jfast.stream;

import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.util.LocalHeap;

public class FASTDynamicWriter {

    private final FASTEncoder writerDispatch;
    private final Pipe ringBuffer;
    final PrimitiveWriter writer;

    public FASTDynamicWriter(PrimitiveWriter primitiveWriter, Pipe ringBuffer, FASTEncoder writerDispatch) {

        this.writerDispatch = writerDispatch;
        this.ringBuffer = ringBuffer;
        this.writer = primitiveWriter;
        
    }

    // this method must never be called unless RingWalker.tryReadFragment(ringBuffer) has returned true
    public static void write(FASTDynamicWriter dynamicWriter) {
    	dynamicWriter.writerDispatch.encode(dynamicWriter.writer, dynamicWriter.ringBuffer);
    }

    public void reset(boolean clearData) {

        writerDispatch.activeScriptCursor = 0;

        if (clearData) {
            writerDispatch.dictionaryFactory.reset(writerDispatch.rIntDictionary);
            writerDispatch.dictionaryFactory.reset(writerDispatch.rLongDictionary);
            LocalHeap.reset(writerDispatch.byteHeap);
        }
    }

}
