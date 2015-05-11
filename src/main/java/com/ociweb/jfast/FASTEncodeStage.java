package com.ociweb.jfast;

import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTOutputRingBuffer;
import com.ociweb.jfast.stream.FASTDynamicWriter;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.loader.TemplateHandler;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class FASTEncodeStage extends PronghornStage {

    private final String template;
    private final RingBuffer input;
    private final RingBuffer output;
    private final int writeBuffer;
    
    private FASTDynamicWriter dynamicWriter;
        
    protected FASTEncodeStage(GraphManager graphManager, RingBuffer input, RingBuffer output, String templatePath) {
        super(graphManager, input, output);
        
        this.template =templatePath;
        this.writeBuffer = output.maxAvgVarLen;
        
        this.input = input;                
        this.output = output;
        
        if (RingBuffer.from(output)!=FieldReferenceOffsetManager.RAW_BYTES) {
            throw new UnsupportedOperationException();
        }
        
        try {
            if (! TemplateHandler.loadFrom(template).equals(RingBuffer.from(input))) {
                throw new UnsupportedOperationException();
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        
    }
    
    @Override
    public void startup() {
                     
        FASTEncoder writerDispatch = DispatchLoader.loadDispatchWriterDebug(TemplateLoader.buildCatBytes(template, new ClientConfig()));

        PrimitiveWriter writer = new PrimitiveWriter(writeBuffer, new FASTOutputRingBuffer(output) , false);
        dynamicWriter = new FASTDynamicWriter(writer, input, writerDispatch);
        
        
    }

    @Override
    public void run() {        
        if (RingReader.tryReadFragment(input)) {
            
            //TODO: create debug statement here that can print fragment should be same code for new stack trace.
            //RingReader.printFragment(input);
           
            
            FASTDynamicWriter.write(dynamicWriter);
                    
            RingReader.releaseReadLock(input);
          //  System.err.println(input);
        } 
    }
    
    @Override
    public void shutdown() {         
    }

}
