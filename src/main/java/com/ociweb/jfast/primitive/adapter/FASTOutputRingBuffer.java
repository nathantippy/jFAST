package com.ociweb.jfast.primitive.adapter;

import static com.ociweb.pronghorn.pipe.Pipe.headPosition;
import static com.ociweb.pronghorn.pipe.Pipe.tailPosition;

import com.ociweb.jfast.primitive.DataTransfer;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.pronghorn.pipe.Pipe;

public class FASTOutputRingBuffer implements FASTOutput {

	private final Pipe ringBuffer;
	private DataTransfer dataTransfer;
    private int fill;
	private long tailPosCache;
		
	public FASTOutputRingBuffer(Pipe ringBuffer) {
		this.ringBuffer = ringBuffer;
		this.fill =  1 + ringBuffer.mask - 2;
		this.tailPosCache = tailPosition(ringBuffer);
	}
	
	@Override
	public void init(DataTransfer dataTransfer) {
		this.dataTransfer = dataTransfer;
	}
	
	@Override
	public void flush() {		
		int size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);
		while (size>0) {
		    
			tailPosCache = Pipe.spinBlockOnTail(tailPosCache, headPosition(ringBuffer)-fill, ringBuffer);			
			Pipe.addMsgIdx(ringBuffer, 0);
			Pipe.addByteArray(dataTransfer.writer.buffer, PrimitiveWriter.nextOffset(dataTransfer.writer), size, ringBuffer);
			size = PrimitiveWriter.nextBlockSize(dataTransfer.writer);		
		}
		Pipe.publishWrites(ringBuffer);		
	}
}