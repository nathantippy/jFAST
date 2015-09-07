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
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
import com.ociweb.pronghorn.pipe.PipeBundle;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitorAdapter;

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

        final int maxMessagesOnRing = 16;
        ClientConfig clientConfig = new ClientConfig();
        
        final String templateSource = getOptArg("templateFile","-t",args,"/performance/example.xml");
        final String dataSource = getOptArg("sourceFile","-s",args,"/performance/complex30000.dat");
        final String stringPreamble = getOptArg("preamble","-p",args,dataSource.endsWith("complex30000.dat")?"4":"0");
        final String stringIterations = getOptArg("iterations","-i",args,"200");
        final String stringMaxTextBytesLength = getOptArg("maxLength","-m",args,"64");
        
        
        clientConfig.setPreableBytes(Integer.valueOf(stringPreamble).shortValue());
        final int targetIterations = Integer.valueOf(stringIterations);
        final int maxStringLength = Integer.valueOf(stringMaxTextBytesLength);       
        
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
            //System.nanoTime()
            
            long finishedCatalogLoad = System.currentTimeMillis();
            
            FASTInputByteArray input = loadFASTInputSourceData(dataSource);
            fileSize = input.remaining();
            totalBytes = fileSize*targetIterations;
                       
            long finishedLoadingRawData = System.currentTimeMillis();
            
            Pipe ringBuffer = buildMessageSpecificRingBuffer(maxMessagesOnRing, maxStringLength, from, byteConst);            
            FASTReaderReactor reactor = FAST.inputReactor(input, catBytes, PipeBundle.buildRingBuffers(ringBuffer));
            
            long finishedCompileOfDecoder = System.currentTimeMillis();
                        
            long iterationsLeft = targetIterations;
            do {  
                long localCount = 0;
                
                long beginDecode = System.currentTimeMillis();
             
                //this pump decodes one message and puts it on the ringBuffer
                while (FASTReaderReactor.pump(reactor)>=0) {
                                        
                    //This would normally be called from a different thread!
                                       
                    //read message off the ring buffer to make room for more messages                    
                    if (PipeReader.tryReadFragment(ringBuffer)) {
                        
                        if (PipeReader.isNewMessage(ringBuffer)) {
                            localCount++;
                            
                            final int msgIdx = PipeReader.getMsgIdx(ringBuffer);
                            long templateId = from.fieldIdScript[msgIdx]; 
                            String templateName = from.fieldNameScript[msgIdx];
                            //
                            //NOTE: If this were a real application using the data, read fields here using LOC for each that was constructed earlier
                            
                        }
                        PipeReader.releaseReadLock(ringBuffer);
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
        System.out.println("AvgDecode   Time : "+(msDecode/(float)targetIterations)+"ms    "+bytesPerMs+"bytes/ms   "+msgPerMs+"msg/ms");
        System.out.println("MaxDecode   Time : "+maxDecode+"ms (before warmup)");
        System.out.println("MinDecode   Time : "+minDecode+"ms (after warmup)");
        
        
        System.out.println("Bytes      Total : "+totalBytes+"   File Size : "+fileSize+"   Iterations: "+targetIterations);
        System.out.println("Messages   Total : "+messageCount);
                
    }
    
    private static String getOptArg(String longName, String shortName, String[] args, String defaultValue) {
        String prev = null;
        for (String token : args) {
            if (longName.equals(prev) || shortName.equals(prev)) {
                if (token == null || token.trim().length() == 0 || token.startsWith("-")) {
                    return defaultValue;
                }
                return token.trim();
            }
            prev = token;
        }
        return defaultValue;
    }


    private static Pipe buildMessageSpecificRingBuffer(
            int maxMessagesOnRing, int maxStringLength,
            FieldReferenceOffsetManager from, byte[] byteConst) {        
        PipeConfig ringConfig = new PipeConfig(from, maxMessagesOnRing, maxStringLength, byteConst);
        Pipe ringBuffer = new Pipe(ringConfig);
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
        
        int pos = 0;
        int len = fileData.length;
        int bytes;
        
        while (pos<len) {
            bytes = inputStream.read(fileData, pos, len-pos);
            pos+=bytes;
        }
        
        inputStream.close();            
        
        byte[] loadFileIntoByteArray = fileData;
        return new FASTInputByteArray(loadFileIntoByteArray);
    }
    
}
