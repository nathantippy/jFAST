package com.ociweb.jfast;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;

import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.TemplateLoader;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.stream.FASTReaderReactor;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.ring.stream.StreamingVisitorReader;

public class TestApp {

    private static final class countingVisitor implements StreamingReadVisitor {
        @Override
        public void visitUnsignedLong(String name, long id, long value) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visitUnsignedInteger(String name, long id, long value) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visitUTF8(String name, long id, Appendable target) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visitTemplateOpen(String name, long id) {
            // TODO Auto-generated method stub
            TestApp.localCount++;
        }

        @Override
        public void visitTemplateClose(String name, long id) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visitSignedLong(String name, long id, long value) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visitSignedInteger(String name, long id, int value) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visitSequenceOpen(String name, long id, int length) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visitSequenceClose(String name, long id) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visitFragmentOpen(String name, long id, int cursor) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visitFragmentClose(String name, long id) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visitDecimal(String name, long id, int exp, long mant) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visitBytes(String name, long id, ByteBuffer value) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void visitASCII(String name, long id, Appendable value) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public Appendable targetUTF8(String name, long id) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ByteBuffer targetBytes(String name, long id, int length) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Appendable targetASCII(String name, long id) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void startup() {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void shutdown() {
            // TODO Auto-generated method stub
            
        }

        @Override
        public boolean paused() {
            // TODO Auto-generated method stub
            return false;
        }
    }


    static int localCount = 0;

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
                
        ByteArrayOutputStream catalogBuffer = new ByteArrayOutputStream(4096);
        
        try {
            
            long start = System.currentTimeMillis();
            
            FieldReferenceOffsetManager from = TemplateLoader.buildCatalog(catalogBuffer, templateSource, clientConfig);
                               
            long finishedCatalogLoad = System.currentTimeMillis();
            
            FASTInputByteArray input = loadFASTInputSourceData(dataSource);
            fileSize = input.remaining();
            totalBytes = fileSize*targetIterators;
                       
            long finishedLoadingRawData = System.currentTimeMillis();
            
            RingBuffer ringBuffer = buildMessageSpecificRingBuffer(maxMessagesOnRing, maxStringLength, from);            
            FASTReaderReactor reactor = FAST.inputReactor(input, catalogBuffer.toByteArray(), RingBuffers.buildRingBuffers(ringBuffer));
            
            long finishedCompileOfDecoder = System.currentTimeMillis();
                        
            long iterationsLeft = targetIterators;
            do {  
                
                StreamingVisitorReader reader = new StreamingVisitorReader(ringBuffer, new countingVisitor());
                                                                                      //BUG: new StreamingReadVisitorToJSON(System.out));
                reader.startup();
                
                long beginDecode = System.currentTimeMillis();
             
                //this pump decodes one message and puts it on the ringBuffer
                while (FASTReaderReactor.pump(reactor)>=0) {
                    
                    //TODO: show how to consume the data.
                    
                    
                    //This would normally be called from a different thead
                    reader.run();
                    
//                    //read message off the ring buffer to make room for more messages                    
//                    if (RingReader.tryReadFragment(ringBuffer)) {
//                        
//                        if (RingReader.isNewMessage(ringBuffer)) {
//                            localCount++;
//                            
//                            final int msgIdx = RingReader.getMsgIdx(ringBuffer);
//                            long templateId = from.fieldIdScript[msgIdx]; 
//                            String templateName = from.fieldNameScript[msgIdx];
//                            
//                            
//                            
//                            
//                        }
//                        RingReader.releaseReadLock(ringBuffer);
//                    } 
                }
                reader.shutdown();
                
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
            FieldReferenceOffsetManager from) {
        RingBufferConfig ringConfig = new RingBufferConfig(from, maxMessagesOnRing, maxStringLength);
        RingBuffer ringBuffer = new RingBuffer(ringConfig);
        ringBuffer.initBuffers();
        return ringBuffer;
    }


    private static FASTInputByteArray loadFASTInputSourceData(String dataSource)
            throws URISyntaxException, IOException {
        File sourceDataFile = new File(dataSource);
        if (!sourceDataFile.exists()) {
            URL sourceData = TestApp.class.getResource(dataSource);
            sourceDataFile = new File(sourceData.toURI());
        }            
        return new FASTInputByteArray(buildInputArrayForTesting(sourceDataFile));
    }

    
    private static byte[] buildInputArrayForTesting(File fileSource) throws IOException {
        byte[] fileData = new byte[(int) fileSource.length()];
        FileInputStream inputStream = new FileInputStream(fileSource);
        int readBytes = inputStream.read(fileData);
        inputStream.close();
        assertEquals(fileData.length, readBytes);

        return fileData;
    }
    
}
