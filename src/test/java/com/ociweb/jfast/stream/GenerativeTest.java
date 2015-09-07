package com.ociweb.jfast.stream;

import java.io.ByteArrayOutputStream;
import java.util.Random;

import org.junit.Test;

import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.jfast.generator.DispatchLoader;
import com.ociweb.jfast.generator.FASTClassLoader;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.stream.StreamingVisitorWriter;
import com.ociweb.pronghorn.pipe.stream.StreamingWriteVisitorGenerator;

public class GenerativeTest {

    private final int varLength = 10;
    private final int messages = 100;
    
    @Test
    public void testGeneratedData() {
        //given known template feed it with generated data to test encoder
        String templateSource = "/performance/example.xml";
        byte[] catBytes = buildRawCatalogData(new ClientConfig(), templateSource);        
        TemplateCatalogConfig catalog = new TemplateCatalogConfig(catBytes);
        FieldReferenceOffsetManager from = catalog.getFROM();
        
        PipeConfig rbConfig = new PipeConfig(from, messages, varLength);
        
        int commonSeed = 300;   
        int iterations = 2;
        
        //This ring contains generated data
        Pipe generatedTestData = buildPopulatedRing(from, rbConfig, commonSeed, iterations);
        
        FASTClassLoader.deleteFiles();
        
        
        int writeBuffer = 16384;
        boolean minimizeLatency = false;
        
        FASTOutput fastOutput = new FASTOutputStream(new ByteArrayOutputStream());
        
        PrimitiveWriter writer = new PrimitiveWriter(writeBuffer, fastOutput , minimizeLatency);
        FASTEncoder writerDispatch = DispatchLoader.loadDispatchWriter(catBytes);
        System.err.println("usingWriter: "+writerDispatch.getClass().getSimpleName());

        
        FASTDynamicWriter dynamicWriter = new FASTDynamicWriter(writer, generatedTestData, writerDispatch);
        
////        TODO: B, this test reveals a problem with sequence of zero, problem was tracked down to tryReadFragment, must fix in that project
//        while (RingReader.tryReadFragment(generatedTestData)) {
////            
//            RingReader.printFragment(generatedTestData);
////////            try{
////////                //write message found on the queue to the output writer
////////                FASTDynamicWriter.write(dynamicWriter);
////////            } catch (FASTException e) {
////////                System.err.println("ERROR: cursor at "+writerDispatch.getActiveScriptCursor()+" "+TokenBuilder.tokenToString(RingBuffer.from(generatedTestData).tokens[writerDispatch.getActiveScriptCursor()]));
////////                throw e;
////////            }
//            RingReader.releaseReadLock(generatedTestData);
//        }
        
        
    }
    
    public Pipe buildPopulatedRing(FieldReferenceOffsetManager from, PipeConfig rbConfig, int commonSeed, int iterations) {
        int i;
        Pipe ring2 = new Pipe(rbConfig);
        ring2.initBuffers();
        StreamingWriteVisitorGenerator swvg2 = new StreamingWriteVisitorGenerator(from, new Random(commonSeed), varLength, varLength);    
        StreamingVisitorWriter svw2 = new StreamingVisitorWriter(ring2, swvg2);
        svw2.startup();     
        i = iterations;
        while (--i>0) {
            svw2.run();
        }
        return ring2;
    }
    
    private static byte[] buildRawCatalogData(ClientConfig clientConfig, String source) {


        ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
        try {
            TemplateLoader.buildCatalog(catalogBuffer, source, clientConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }

        assert(catalogBuffer.size() > 0);

        byte[] catalogByteArray = catalogBuffer.toByteArray();
        return catalogByteArray;
    }
    
}
