package com.ociweb.jfast;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;

import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.stream.FASTReaderReactor;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.stream.StreamingReadVisitorAdapter;

public class TestApp {

    private static final class countingVisitor extends StreamingReadVisitorAdapter {

        private int localCount = 0;
        
        @Override
        public void visitTemplateOpen(String name, long id) {
            localCount++;
        }
        
        public int messageCount() {
            return localCount;
        }

    }



    public static void main(String[] args) {

        ClientConfig clientConfig = new ClientConfig();
        
        clientConfig.setPreableBytes((short)4);
        String templateSource = "/performance/example.xml";
        String dataSource = "/performance/complex30000.dat";
        int maxMessagesOnRing = 100;
        int maxStringLength = 8;
        long targetIterators = 200;
                
        //TODO: AAAA, add args the same as mFAST application.
        
        
        long messageCount = 0;        
        long msCatLoad = -1;
        long msDiskIO = -1;
        long msCompile = -1;
        long msDecode = -1;
        long totalBytes = -1;
        int fileSize = -1;
        long maxDecode = Long.MIN_VALUE;
        long minDecode = Long.MAX_VALUE;
                
        
        try {
            
            long start = System.currentTimeMillis();
            
            ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
            FieldReferenceOffsetManager from = TemplateLoader.buildCatalog(catalogBuffer, templateSource, clientConfig);
            byte[] catBytes = catalogBuffer.toByteArray();
            byte[] byteConst = new TemplateCatalogConfig(catBytes).ringByteConstants();            
            
            long finishedCatalogLoad = System.currentTimeMillis();
            
            FASTInputByteArray input = loadFASTInputSourceData(dataSource);
            fileSize = input.remaining();
            totalBytes = fileSize*targetIterators;
                       
            long finishedLoadingRawData = System.currentTimeMillis();
            
            RingBuffer ringBuffer = buildMessageSpecificRingBuffer(maxMessagesOnRing, maxStringLength, from, byteConst);            
            FASTReaderReactor reactor = FAST.inputReactor(input, catBytes, RingBuffers.buildRingBuffers(ringBuffer));
            
            long finishedCompileOfDecoder = System.currentTimeMillis();
                        
            long localCount = 0;
            long iterationsLeft = targetIterators;
            do {  
                
                long beginDecode = System.currentTimeMillis();
             
                //this pump decodes one message and puts it on the ringBuffer
                while (FASTReaderReactor.pump(reactor)>=0) {
                    
                    //TODO: show how to consume the data.
                    
                    
                    //This would normally be called from a different thread!
                                       
                    //read message off the ring buffer to make room for more messages                    
                    if (RingReader.tryReadFragment(ringBuffer)) {
                        
                        if (RingReader.isNewMessage(ringBuffer)) {
                            localCount++;
                            
                            final int msgIdx = RingReader.getMsgIdx(ringBuffer);
                            long templateId = from.fieldIdScript[msgIdx]; 
                            String templateName = from.fieldNameScript[msgIdx];
                            //
                            //NOTE: If this were a real application using the data, read fields here using LOC for each that was constructed earlier
                            
                        }
                        RingReader.releaseReadLock(ringBuffer);
                    } 
                }
                
                long decodeDuration = System.currentTimeMillis()-beginDecode;
                minDecode = Math.min(minDecode, decodeDuration);
                maxDecode = Math.max(maxDecode, decodeDuration);
                
                messageCount += localCount;
                input.reset();//restart at the beginning of the input data for next trip and/or final count
                
            } while (--iterationsLeft>0);
            
            long finished =  System.currentTimeMillis();
            
            msCatLoad = finishedCatalogLoad - start;
            msDiskIO = finishedLoadingRawData - finishedCatalogLoad;
            msCompile = finishedCompileOfDecoder - finishedLoadingRawData;
            msDecode = finished - finishedCompileOfDecoder;            
            
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        
        float msgPerMs = messageCount/(float)msDecode;
        float bytesPerMs = totalBytes/(float)msDecode;
        
        
        System.out.println("CatalogLoad Time : "+msCatLoad+"ms");
        System.out.println("DiskIO      Time : "+msDiskIO+"ms");
        System.out.println("Compile     Time : "+msCompile+"ms");
        System.out.println("AvgDecode   Time : "+(msDecode/(float)targetIterators)+"ms    "+bytesPerMs+"bytes/ms   "+msgPerMs+"msg/ms");
        System.out.println("MaxDecode   Time : "+maxDecode+"ms (before warmup)");
        System.out.println("MinDecode   Time : "+minDecode+"ms (after warmup)");
        
        
        System.out.println("Bytes      Total : "+totalBytes+"   File Size : "+fileSize+"   Iterations: "+targetIterators);
        System.out.println("Messages   Total : "+messageCount);
                
    }


    private static RingBuffer buildMessageSpecificRingBuffer(
            int maxMessagesOnRing, int maxStringLength,
            FieldReferenceOffsetManager from, byte[] byteConst) {        
        RingBufferConfig ringConfig = new RingBufferConfig(from, maxMessagesOnRing, maxStringLength, byteConst);
        RingBuffer ringBuffer = new RingBuffer(ringConfig);
        ringBuffer.initBuffers();
        return ringBuffer;
    }


    private static FASTInputByteArray loadFASTInputSourceData(String dataSource)
            throws URISyntaxException, IOException {
        File sourceDataFile = new File(dataSource);
        byte[] fileData;
        InputStream inputStream;
        
        if (!sourceDataFile.exists()) {
            inputStream = TestApp.class.getResourceAsStream(dataSource);
            fileData = new byte[inputStream.available()];
        } else {
            fileData = new byte[(int) sourceDataFile.length()];
            inputStream = new FileInputStream(sourceDataFile);
        }
        inputStream.read(fileData);
        inputStream.close();            
        byte[] loadFileIntoByteArray = fileData;
        return new FASTInputByteArray(loadFileIntoByteArray);
    }
    
}
