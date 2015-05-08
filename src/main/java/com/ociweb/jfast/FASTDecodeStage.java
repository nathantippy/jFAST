package com.ociweb.jfast;

import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.stream.FASTReaderReactor;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.loader.TemplateHandler;
import com.ociweb.pronghorn.ring.stream.RingInputStream;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class FASTDecodeStage extends PronghornStage {

    private final String template;
    private final RingBuffer input;
    private final RingBuffer output;
    
    private FASTReaderReactor reactor;
    
    protected FASTDecodeStage(GraphManager graphManager, RingBuffer input, RingBuffer output, String templatePath) {
        super(graphManager, input, output);
        
        this.template = templatePath;   
        this.input = input;                
        this.output = output;

        if (RingBuffer.from(input)!=FieldReferenceOffsetManager.RAW_BYTES) {
            throw new UnsupportedOperationException();
        }
                
        try {
            if (! TemplateHandler.loadFrom(template).equals(RingBuffer.from(output))) {
                throw new UnsupportedOperationException();
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        
    }

    @Override
    public void startup() {
        
        //memory allocation done in these methods, do not move from startup()
        byte[] catBytes = TemplateLoader.buildCatBytes(template, new ClientConfig());
        RingInputStream inputStream = new RingInputStream(input);
        FASTInput fastInput = new FASTInputStream(inputStream);//TODO: AAA, replace this with direct RingBuffer implementation
        reactor = FAST.inputReactorDebug(fastInput, catBytes, RingBuffers.buildRingBuffers(output));
        
    }
    
    
    @Override
    public void run() {
        int count = 10;
        if (--count>0 && FASTReaderReactor.pump(reactor)<0) {
            requestShutdown();
        }
    }

}
