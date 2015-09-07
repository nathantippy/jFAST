package com.ociweb.jfast.stream;

import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;

public class TestHelper {

    public static long readLong(int token, PrimitiveReader reader, Pipe ringBuffer, FASTReaderInterpreterDispatch readerInterpreterDispatch) {
    
        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
    
        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            // not optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readerInterpreterDispatch.readLongUnsigned(token, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            } else {
                readerInterpreterDispatch.readLongSigned(token, readerInterpreterDispatch.rLongDictionary, readerInterpreterDispatch.MAX_LONG_INSTANCE_MASK, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            }
        } else {
            // optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readerInterpreterDispatch.readLongUnsignedOptional(token, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            } else {
                readerInterpreterDispatch.readLongSignedOptional(token, readerInterpreterDispatch.rLongDictionary, readerInterpreterDispatch.MAX_LONG_INSTANCE_MASK, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            }
        }
        //NOTE: for testing we need to check what was written
        return Pipe.peekLong(Pipe.primaryBuffer(ringBuffer), Pipe.workingHeadPosition(ringBuffer)-2, ringBuffer.mask);
    }

    public static int readInt(int token, PrimitiveReader reader, Pipe ringBuffer, FASTReaderInterpreterDispatch readerInterpreterDispatch) {
    
        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            // not optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readerInterpreterDispatch.readIntegerUnsigned(token, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            } else {
                readerInterpreterDispatch.readIntegerSigned(token, readerInterpreterDispatch.rIntDictionary, readerInterpreterDispatch.MAX_INT_INSTANCE_MASK, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            }
        } else {
            // optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readerInterpreterDispatch.readIntegerUnsignedOptional(token, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            } else {
                readerInterpreterDispatch.readIntegerSignedOptional(token, readerInterpreterDispatch.rIntDictionary, readerInterpreterDispatch.MAX_INT_INSTANCE_MASK, readerInterpreterDispatch.readFromIdx, reader, ringBuffer);
            }
        }
        //NOTE: for testing we need to check what was written
        return Pipe.peek(Pipe.primaryBuffer(ringBuffer), Pipe.workingHeadPosition(ringBuffer)-1, ringBuffer.mask);
    }

}
